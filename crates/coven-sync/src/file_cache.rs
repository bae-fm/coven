//! Pins, budgets and downloads driven by committed file rows.

use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use crate::{LocalFailure, PendingReason, PendingRecord, PendingSubject, Prerequisite};
use coven_database::{DbError, FileRef, LiveQueryClosed, RetrySubject, RetryWait, RowKey};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Plaintext download progress; authentication tags and headers are excluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinProgress {
    /// Files fully read and checked.
    pub files_completed: u64,
    /// Requested file count, including present files.
    pub files_total: u64,
    /// Missing plaintext bytes downloaded by this call.
    pub bytes_downloaded: u64,
    /// Plaintext bytes missing when this call began.
    pub bytes_total: u64,
}
/// Observable eager-download state (E8).
#[derive(Clone, Debug)]
pub enum EagerCacheFillStatus {
    /// No downloads are waiting.
    Idle,
    /// A committed row's file is downloading.
    Downloading(PinProgress),
    /// The application cancelled the pending downloads.
    Cancelled(PinProgress),
}
/// The eager references a fill has attempted this session. A failed one is
/// attempted again only once its persisted retry wait is due and no provider
/// cooldown holds the connection (§16.5, SP waiting); a wake never requests
/// an unchanged wait again.
#[derive(Default)]
pub(crate) struct EagerAttempts {
    seen: Vec<FileRef>,
}
/// The persisted wait of an eager download of `file`.
pub(crate) fn download_subject(file: &FileRef) -> RetrySubject {
    RetrySubject::Download {
        uploader: file.uploader(),
        file: file.id(),
    }
}
/// Pin answers with replaceable row requests.
pub struct RowsPinnedLiveQuery {
    files: Files,
    request: Mutex<(String, Vec<RowKey>)>,
    requested: tokio::sync::Notify,
    changes: watch::Receiver<u64>,
    commits: coven_database::DatabaseChanges,
    first: bool,
    previous: Option<Vec<Option<bool>>>,
}
impl RowsPinnedLiveQuery {
    /// Replace the watched rows, retaining their supplied order.
    pub fn set_rows(&self, table: &str, keys: Vec<RowKey>) -> Result<(), LiveQueryClosed> {
        self.files.inner.check_open().map_err(|_| LiveQueryClosed)?;
        *self.request.lock().expect("pin query poisoned") = (table.into(), keys);
        self.requested.notify_one();
        Ok(())
    }
    /// Current answers immediately, then changes from row writes or cache actions.
    pub async fn next(&mut self) -> Result<Vec<Option<bool>>, FileReadError> {
        loop {
            if !self.first {
                tokio::select! {
                    _=self.requested.notified()=>{},
                    result=self.changes.changed()=>{result.map_err(|_|DbError::StoreClosed)?;},
                    result=self.commits.next()=>result?,
                }
            }
            self.first = false;
            self.changes.borrow_and_update();
            let (table, keys) = self.request.lock().expect("pin query poisoned").clone();
            let value = self.files.rows_pinned(&table, keys).await?;
            if self.previous.as_ref() != Some(&value) {
                self.previous = Some(value.clone());
                return Ok(value);
            }
        }
    }
}
impl Files {
    /// Download missing chunks and retain complete files regardless of budgets.
    pub async fn pin(
        &self,
        files: &[FileRef],
        on_progress: &(dyn Fn(PinProgress) + Send + Sync),
    ) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let limit = self.transfer_limits().downloads.get();
        let progress = Mutex::new(self.inner.progress(files).await?);
        on_progress(progress.lock().expect("pin progress poisoned").clone());
        futures_util::stream::iter(files.to_vec())
            .map(|file| {
                let progress = &progress;
                async move {
                    // A file still queued here is already whole on this device.
                    match self.inner.database.open_local(&file).await? {
                        Some(_) => {}
                        None => {
                            if !self.inner.database.pin_complete(&file).await? {
                                let uploaded =
                                    UploadedFile::open(self.inner.clone(), file.clone()).await?;
                                let previous = Mutex::new(0);
                                uploaded
                                    .keep_whole(|downloaded| {
                                        let mut previous =
                                            previous.lock().expect("file progress poisoned");
                                        let mut progress =
                                            progress.lock().expect("pin progress poisoned");
                                        progress.bytes_downloaded += downloaded - *previous;
                                        *previous = downloaded;
                                        on_progress(progress.clone());
                                    })
                                    .await?;
                            }
                        }
                    }
                    let mut progress = progress.lock().expect("pin progress poisoned");
                    progress.files_completed += 1;
                    on_progress(progress.clone());
                    self.inner.notify();
                    Ok::<_, FileReadError>(())
                }
            })
            .buffer_unordered(limit)
            .try_for_each(|()| std::future::ready(Ok(())))
            .await
    }
    /// Release budget exemptions, then apply each affected namespace budget.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        for file in files {
            self.inner.database.unpin(file).await?;
            self.inner.trim(file.namespace()).await;
        }
        self.inner.notify();
        Ok(())
    }
    /// Pin answers in key order; an absent row or unattached file is `None`.
    pub async fn rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> Result<Vec<Option<bool>>, FileReadError> {
        self.inner.check_open()?;
        let mut answers = Vec::new();
        for key in keys {
            match self.inner.database.file_ref(table, key).await {
                Ok(file) => answers.push(Some(self.inner.database.is_pinned(&file).await?)),
                Err(DbError::FileAbsent { .. }) => answers.push(None),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(answers)
    }
    /// Observe pin changes and the database's committed file-column changes.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> RowsPinnedLiveQuery {
        RowsPinnedLiveQuery {
            files: self.clone(),
            request: Mutex::new((table.into(), keys)),
            requested: tokio::sync::Notify::new(),
            changes: self.inner.changed.subscribe(),
            commits: self.inner.database.changes(),
            first: true,
            previous: None,
        }
    }
    /// Remove cached bytes only, including pinned bytes; storage is untouched.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        self.inner.database.evict(file).await?;
        self.inner.notify();
        Ok(())
    }
    /// Set the budget and immediately evict this namespace's unpinned LRU bytes.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.inner.check_open()?;
        let _cache = self.inner.cache.write().await;
        self.inner.database.set_budget(namespace, max_bytes).await?;
        self.inner.database.trim_cache(namespace).await?;
        self.inner.notify();
        Ok(())
    }
    /// Unconfigured namespaces have no eviction budget.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        self.inner.check_open()?;
        self.inner.database.budget(namespace).await
    }
    /// Observe downloads requested by CacheEager declarations.
    pub fn subscribe_eager_cache_fill_status(&self) -> watch::Receiver<EagerCacheFillStatus> {
        self.inner.eager.subscribe()
    }
    /// Cancel the active eager download; later arriving rows remain eligible.
    pub fn cancel_eager_cache_fill(&self) {
        self.inner
            .cancel_eager
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}
impl FilesInner {
    async fn progress(&self, files: &[FileRef]) -> Result<PinProgress, DbError> {
        let mut bytes_total = 0u64;
        for file in files {
            bytes_total = bytes_total
                .checked_add(self.database.missing_bytes(file).await?)
                .ok_or(DbError::TooLarge {
                    field: "pin bytes",
                    actual: u64::MAX,
                    maximum: u64::MAX,
                })?;
        }
        Ok(PinProgress {
            files_completed: 0,
            files_total: files.len() as u64,
            bytes_downloaded: 0,
            bytes_total,
        })
    }
    pub(crate) async fn fill_eager(self: &Arc<Self>) {
        let mut attempts = self.eager_attempts.lock().await;
        if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return;
        }
        let files = match self.database.eager_files().await {
            Ok(files) => files,
            Err(error) => {
                // No file subject holds this failure; the next wake retries it.
                tracing::error!(?error, "listing eager files failed");
                return;
            }
        };
        let waits: std::collections::BTreeMap<_, _> = match self.database.download_waits().await {
            Ok(waits) => waits.into_iter().collect(),
            Err(error) => {
                // No file subject holds this failure; the next wake retries it.
                tracing::error!(?error, "reading eager download waits failed");
                return;
            }
        };
        // A new reference, or a failed one whose wait is due; neither while
        // a provider cooldown holds the connection.
        let pending = files
            .iter()
            .filter(|file| {
                let subject = download_subject(file);
                let persisted = waits.get(&subject);
                (persisted.is_some() || !attempts.seen.contains(file))
                    && self.waits.due(subject, persisted)
            })
            .cloned()
            .collect::<Vec<_>>();
        attempts.seen.retain(|file| files.contains(file));
        for file in &pending {
            if !attempts.seen.contains(file) {
                attempts.seen.push(file.clone());
            }
        }
        if pending.is_empty() {
            return;
        }
        let mut progress = match self.progress(&pending).await {
            Ok(p) => p,
            Err(error) => {
                tracing::error!(?error, "measuring eager downloads failed");
                attempts.seen.retain(|file| !pending.contains(file));
                return;
            }
        };
        let mut cancel = self.cancel_eager.subscribe();
        self.eager
            .send_replace(EagerCacheFillStatus::Downloading(progress.clone()));
        for file in &pending {
            let current = Mutex::new(progress.clone());
            let work = async {
                self.database.validate(file).await?;
                let upload = UploadedFile::open(self.clone(), file.clone()).await?;
                upload
                    .download(|bytes| {
                        let mut p = current.lock().expect("eager progress poisoned");
                        p.bytes_downloaded = progress.bytes_downloaded + bytes;
                        self.eager
                            .send_replace(EagerCacheFillStatus::Downloading(p.clone()));
                    })
                    .await
            };
            let result = tokio::select! {
                _ = cancel.changed() => {
                    // Cancelled downloads are not retried; new references remain eligible.
                    for file in &pending {
                        if let Err(error) = self.waits.clear(download_subject(file)).await {
                            tracing::error!(?error, "ending a cancelled download's wait failed");
                        }
                    }
                    self.eager.send_replace(EagerCacheFillStatus::Cancelled(
                        current.into_inner().expect("eager progress poisoned"),
                    ));
                    return;
                }
                result = work => result,
            };
            progress = current.into_inner().expect("eager progress poisoned");
            // One file's failure is that file's pending record; the rest of
            // the fill continues (§16.4, E8).
            let reason = match result {
                Ok(Ok(())) => None,
                Ok(Err(refusal)) => Some(PendingReason::Refused(refusal)),
                Err(error) => Some(self.read_pending(file, &error)),
            };
            let subject = PendingSubject::File {
                device: file.uploader(),
                file: file.id(),
            };
            let retry = download_subject(file);
            match reason {
                None => {
                    progress.files_completed += 1;
                    // Its wait belongs to the record and goes with it.
                    match self.database.clear_file_pending(subject).await {
                        Ok(()) => self.waits.forget(retry),
                        Err(error) => tracing::error!(
                            ?error,
                            "clearing a completed eager download's record failed"
                        ),
                    }
                }
                Some(reason) => {
                    self.record_download_failure(subject, retry, reason, waits.get(&retry))
                        .await
                }
            }
        }
        self.eager.send_replace(EagerCacheFillStatus::Idle);
    }

    /// Persist the failed download's record with its next wait before it
    /// waits (§16.5): temporary conditions retry once the wait is due,
    /// others wait for the app or an update and keep no wait. A wait that
    /// cannot be chosen or persisted holds the download until the app acts.
    async fn record_download_failure(
        &self,
        subject: PendingSubject,
        retry: RetrySubject,
        reason: PendingReason,
        previous: Option<&RetryWait>,
    ) {
        let timed = retries(&reason);
        let wait = if timed {
            match self.waits.next(previous) {
                Ok(wait) => Some(wait),
                Err(error) => {
                    tracing::error!(?error, "choosing an eager download's retry wait failed");
                    None
                }
            }
        } else {
            None
        };
        let recorded = self
            .database
            .fail_download(
                move |reported_by| PendingRecord {
                    subject,
                    reason,
                    reported_by,
                },
                retry,
                wait,
            )
            .await;
        match (recorded, wait) {
            (Ok(()), Some(wait)) => self.waits.arm(retry, &wait),
            (Ok(()), None) if !timed => self.waits.forget(retry),
            (Ok(()), None) => self.waits.hold(retry),
            (Err(error), _) => {
                tracing::error!(?error, "recording an eager download's pending state failed");
                self.waits.hold(retry);
            }
        }
    }

    /// The first unmet condition a failed read of `file` leaves (§16.3).
    fn read_pending(&self, file: &FileRef, error: &FileReadError) -> PendingReason {
        match error {
            FileReadError::NoStorage => PendingReason::NoStorage,
            FileReadError::Storage(error) => {
                match self.storage.read().expect("storage lock poisoned").as_ref() {
                    Some(storage) => PendingReason::Storage {
                        provider: storage.config().provider(),
                        failure: error.failure(),
                    },
                    None => PendingReason::NoStorage,
                }
            }
            FileReadError::Uploading { .. } => PendingReason::Waits(Prerequisite::Object(
                coven_storage::ObjectPath::file(self.directory.id(), file.uploader(), file.id()),
            )),
            FileReadError::Missing { reason, .. } => PendingReason::FileUnavailable(reason.clone()),
            FileReadError::Integrity { .. } => {
                PendingReason::Refused(crate::Refusal::Decryption { cause: None })
            }
            FileReadError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            FileReadError::Disk(_) | FileReadError::Lock(_) => {
                PendingReason::Failed(LocalFailure::Disk)
            }
            FileReadError::Database(_)
            | FileReadError::UserFileMissing { .. }
            | FileReadError::UserFileChanged { .. }
            | FileReadError::RangeOutOfBounds { .. } => {
                PendingReason::Failed(LocalFailure::Database)
            }
        }
    }
}

/// Temporary conditions retry on a later pass; refusals of immutable bytes,
/// a missing file and a required update wait for app action or an update.
fn retries(reason: &PendingReason) -> bool {
    !matches!(
        reason,
        PendingReason::Refused(_)
            | PendingReason::FileUnavailable(_)
            | PendingReason::UpdateRequired(_)
    )
}
