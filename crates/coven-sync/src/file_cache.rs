//! Pins, budgets and downloads driven by committed file rows.

use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use coven_database::{DbError, FileLocation, FileRef, LiveQueryClosed, RowKey};
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
    /// The current download failed, retaining its typed cause.
    Failed {
        /// Progress before failure.
        progress: PinProgress,
        /// Original error.
        error: Arc<FileReadError>,
    },
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
                    self.inner.database.validate(&file).await?;
                    match file.location() {
                        FileLocation::OnDevice(_) => {
                            self.inner.database.open_local(&file).await?;
                        }
                        FileLocation::Uploaded => {
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
        let mut seen = self.eager_seen.lock().await;
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
                self.eager.send_replace(EagerCacheFillStatus::Failed {
                    progress: PinProgress {
                        files_completed: 0,
                        files_total: 0,
                        bytes_downloaded: 0,
                        bytes_total: 0,
                    },
                    error: Arc::new(error.into()),
                });
                return;
            }
        };
        let pending = files
            .iter()
            .filter(|file| !seen.contains(file))
            .cloned()
            .collect::<Vec<_>>();
        *seen = files;
        if pending.is_empty() {
            return;
        }
        let mut progress = match self.progress(&pending).await {
            Ok(p) => p,
            Err(error) => {
                self.eager.send_replace(EagerCacheFillStatus::Failed {
                    progress: PinProgress {
                        files_completed: 0,
                        files_total: pending.len() as u64,
                        bytes_downloaded: 0,
                        bytes_total: 0,
                    },
                    error: Arc::new(error.into()),
                });
                return;
            }
        };
        let mut cancel = self.cancel_eager.subscribe();
        self.eager
            .send_replace(EagerCacheFillStatus::Downloading(progress.clone()));
        for (index, file) in pending.iter().enumerate() {
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
            tokio::select! {
                _=cancel.changed()=>{self.eager.send_replace(EagerCacheFillStatus::Cancelled(current.into_inner().expect("eager progress poisoned")));return},
                result=work=>if let Err(error)=result {seen.retain(|file| !pending[index..].contains(file));self.eager.send_replace(EagerCacheFillStatus::Failed{progress:current.into_inner().expect("eager progress poisoned"),error:Arc::new(error)});return},
            }
            progress = current.into_inner().expect("eager progress poisoned");
            progress.files_completed += 1;
        }
        self.eager.send_replace(EagerCacheFillStatus::Idle);
    }
}
