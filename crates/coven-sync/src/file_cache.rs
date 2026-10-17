//! Pins, budgets and downloads driven by committed file rows.

use super::file_cache_retry::{download_clears, download_subject};
use super::file_error::TransferError;
use crate::pending::Clears;
use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use crate::{PendingReason, PendingSubject};
use coven_database::{DbError, FileRef};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;

impl Files {
    /// Download missing chunks and retain complete files regardless of
    /// budgets. Progress is the committed coverage `pin_progress` reads. A
    /// file still queued here is whole already; its upload carries the pin.
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
    /// Commit cancellation of the requested eager downloads, then stop the
    /// active fill; later changed references remain eligible.
    pub async fn cancel_eager_cache_fill(&self) -> Result<(), DbError> {
        self.inner.check_open()?;
        let retired = self.inner.database.cancel_eager().await?;
        for subject in retired {
            self.inner.waits.forget(subject);
            self.inner.forget_connection_wait(subject);
        }
        self.inner
            .cancel_eager
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }
}
impl FilesInner {
    /// SP, Files and waiting. One file's failure is that file's pending record; a failure
    /// no file subject holds, such as reading the work, fails the fill.
    pub(crate) async fn fill_eager(self: &Arc<Self>) -> Result<(), crate::SyncError> {
        let _fill = self.eager_fill.lock().await;
        if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return Ok(());
        }
        self.fill_pins().await?;
        let files = self.database.eager_work().await?;
        if files.is_empty() {
            return Ok(());
        }
        let waits: std::collections::BTreeMap<_, _> =
            self.database.download_waits().await?.into_iter().collect();
        // A changed reference, or a failed one whose wait is due; neither
        // while a provider cooldown holds the connection.
        let pending = files
            .into_iter()
            .filter(|file| {
                let subject = download_subject(file);
                self.connection_due(subject) && self.waits.due(subject, waits.get(&subject))
            })
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        // The reservations are the fill's progress and cancellation (§16.4).
        self.database.request_eager(pending.clone()).await?;
        let mut cancel = self.cancel_eager.subscribe();
        for file in &pending {
            self.forget_connection_wait(download_subject(file));
            let work = async {
                self.database.validate(file).await?;
                UploadedFile::open(self.clone(), file.clone())
                    .await?
                    .download()
                    .await
            };
            let result = tokio::select! {
                _ = cancel.changed() => {
                    // Cancelled downloads are not retried; new references remain eligible.
                    for file in &pending {
                        self.waits.clear(download_subject(file)).await?;
                        self.forget_connection_wait(download_subject(file));
                    }
                    self.database.settle_eager_work(pending.clone()).await?;
                    return Ok(());
                }
                result = work => result,
            };
            // One file's failure is that file's pending record; the rest of
            // the fill continues (§16.4, E8).
            let reason = match result {
                Ok(()) => None,
                Err(TransferError::Refused(refusal)) => {
                    // Authenticated chunks that are not this file publish no
                    // completed file: they leave the cache with the refusal.
                    self.database.evict(file).await?;
                    Some(PendingReason::Refused(refusal))
                }
                Err(error) => Some(self.download_reason(file, &error)),
            };
            // Recoverable failures keep their work, whether waiting for a
            // timer or a new connection. Only terminal outcomes settle it.
            if reason
                .as_ref()
                .is_none_or(|reason| download_clears(reason) == Clears::Never)
            {
                self.database.settle_eager_work(vec![file.clone()]).await?;
            }
            self.finish_download(file, reason, true).await?;
        }
        Ok(())
    }

    /// Pins carried from completed uploads (§16.4): each file is kept whole
    /// from storage as an app's pin is. One file's failure is its own
    /// pending record; a pin whose row no longer names its file has nothing
    /// to keep.
    async fn fill_pins(self: &Arc<Self>) -> Result<(), crate::SyncError> {
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
            let subject = PendingSubject::File {
                device: file.uploader(),
                file: file.id(),
            };
            match kept {
                Ok(()) => {
                    self.finish_download(&file, None, true).await?;
                }
                Err(TransferError::File(FileReadError::Database(
                    DbError::FileRefChanged { .. } | DbError::FileAbsent { .. },
                ))) => self.database.settle_pin_request(file).await?,
                Err(error) => {
                    let reason = self.download_reason(&file, &error);
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
