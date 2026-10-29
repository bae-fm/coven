//! Pins, budgets and downloads driven by committed file rows.

use super::file_cache_retry::{download_clears, download_subject};
use super::file_error::TransferError;
use super::file_failure::{file_subject, FileCopy, FileFailure};
use crate::pending::Clears;
use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use coven_database::{CacheReservation, DbError, FileRef};
use coven_foundation::files::{FileArea, FileName, FileWriter, StoreDirAccess};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;

impl Files {
    /// Download missing chunks and retain complete files regardless of
    /// budgets. Progress is the committed coverage `pin_progress` reads. A
    /// file still queued here is whole already; its upload keeps the pin.
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let limit = self.transfer_limits().downloads.get();
        futures_util::stream::iter(files.to_vec())
            .map(|file| async move {
                let work = async {
                    match self.inner.database.pin_complete(&file).await? {
                        Some(coven_database::PinnedFile::Queued) => return Ok(false),
                        Some(coven_database::PinnedFile::Cached) => return Ok(true),
                        None => {}
                    }
                    UploadedFile::open(self.inner.clone(), file.clone())
                        .await?
                        .keep_whole()
                        .await?;
                    Ok::<_, TransferError>(true)
                }
                .await;
                // A queued local file's pin is a reservation; it did not read
                // or verify the source and cannot settle its pending upload.
                if matches!(work, Ok(false)) {
                    return Ok(());
                }
                self.inner
                    .complete_download(&file, work.map(|_| ()), true)
                    .await
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
        Ok(())
    }
    /// Remove cached bytes only, including pinned bytes; storage is untouched.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        self.inner.database.evict(file).await?;
        Ok(())
    }
    /// Set the budget and immediately evict this namespace's unpinned LRU bytes.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.inner.check_open()?;
        let _cache = self.inner.cache.write().await;
        self.inner.database.set_budget(namespace, max_bytes).await?;
        self.inner.database.trim_cache(namespace).await?;
        Ok(())
    }
    /// The namespace's budget, declared or since set (E8).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError> {
        self.inner.check_open()?;
        self.inner.database.budget(namespace).await
    }
}
impl FilesInner {
    /// Reserve a whole cache file of `file` and create it for writing. The
    /// writer syncs its bytes on finishing; the reservation then publishes
    /// them, and a dropped one leaves them to their recorded removal.
    pub(crate) async fn create_cache_file(
        &self,
        file: &FileRef,
    ) -> Result<(CacheReservation, FileWriter), FileReadError> {
        let name = FileName::new(self.ids.new_id().to_string()).expect("UUID cache name");
        let reservation = self.database.reserve_cache_file(file, name.clone()).await?;
        let target = self.directory.file(FileArea::Cache, &name);
        let directory = self.directory.clone();
        // Cleanup cannot release the name while creation still runs after a
        // cancelled await; the blocking task retains its reservation.
        let (reservation, writer) = crate::files::join(tokio::task::spawn_blocking(move || {
            let writer = (|| -> Result<_, FileReadError> {
                let lock = directory.lock_read_only()?;
                Ok(target.create_writer(lock)?)
            })();
            (reservation, writer)
        }))
        .await;
        Ok((reservation, writer?))
    }
    /// Pins carried from completed uploads (§17.6, §6.7): each file is kept
    /// whole from storage as an app's pin is. One file's failure is its own
    /// pending record; a failure no file subject holds, such as reading the
    /// work, fails the run. A pin whose row no longer names its file has
    /// nothing to keep.
    pub(crate) async fn keep_pins(self: &Arc<Self>) -> Result<(), crate::SyncError> {
        let _run = self.pin_run.lock().await;
        if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return Ok(());
        }
        let files = self.database.pin_requests().await?;
        if files.is_empty() {
            return Ok(());
        }
        let waits: std::collections::BTreeMap<_, _> =
            self.database.download_waits().await?.into_iter().collect();
        let _pins = self.pins.lock().await;
        for file in files {
            let retry = download_subject(&file);
            if !self.connection_due(retry) || !self.waits.due(retry, waits.get(&retry)) {
                continue;
            }
            self.forget_connection_wait(retry);
            let kept = async {
                UploadedFile::open(self.clone(), file.clone())
                    .await?
                    .keep_whole()
                    .await
            }
            .await;
            match kept {
                Ok(()) => {
                    self.finish_download(&file, None, true).await?;
                }
                Err(TransferError::File(FileReadError::Database(
                    DbError::FileRefChanged { .. } | DbError::FileAbsent { .. },
                ))) => self.database.settle_pin_request(file).await?,
                Err(error) => {
                    let reason =
                        self.file_failure(&file, FileCopy::Stored, FileFailure::Held(error));
                    let subject = file_subject(&file);
                    if download_clears(&reason) == Clears::Never {
                        self.database.settle_pin_request(file).await?;
                    }
                    self.record_download_failure(subject, retry, reason, waits.get(&retry))
                        .await?;
                }
            }
        }
        Ok(())
    }
}
