//! Pins, budgets and downloads driven by committed file rows.

use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use crate::{LocalFailure, PendingReason, PendingRecord, PendingSubject, Prerequisite};
use coven_database::{DbError, FileRef, RetrySubject, RetryWait};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;

/// The persisted wait of an eager download of `file`.
pub(crate) fn download_subject(file: &FileRef) -> RetrySubject {
    RetrySubject::Download {
        uploader: file.uploader(),
        file: file.id(),
    }
}
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
                if !self.inner.database.pin_complete(&file).await? {
                    UploadedFile::open(self.inner.clone(), file.clone())
                        .await?
                        .keep_whole()
                        .await?;
                }
                Ok::<_, FileReadError>(())
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
        self.inner.database.cancel_eager().await?;
        self.inner
            .cancel_eager
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }
}
impl FilesInner {
    /// SP step 7. One file's failure is that file's pending record; a failure
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
                self.waits.due(subject, waits.get(&subject))
            })
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        // The reservations are the fill's progress and cancellation (§16.4).
        self.database.request_eager(pending.clone()).await?;
        let mut cancel = self.cancel_eager.subscribe();
        for file in &pending {
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
                    }
                    self.database.settle_eager_work(pending.clone()).await?;
                    return Ok(());
                }
                result = work => result,
            };
            // One file's failure is that file's pending record; the rest of
            // the fill continues (§16.4, E8).
            let reason = match result {
                Ok(Ok(())) => None,
                Ok(Err(refusal)) => {
                    // Authenticated chunks that are not this file publish no
                    // completed file: they leave the cache with the refusal.
                    self.database.evict(file).await?;
                    Some(PendingReason::Refused(refusal))
                }
                Err(error) => Some(self.read_pending(file, &error)),
            };
            let subject = PendingSubject::File {
                device: file.uploader(),
                file: file.id(),
            };
            let retry = download_subject(file);
            // Done or permanently refused, the work is settled; a temporary
            // failure keeps it for its retry wait.
            if reason.as_ref().is_none_or(|reason| !retries(reason)) {
                self.database.settle_eager_work(vec![file.clone()]).await?;
            }
            match reason {
                None => {
                    // Its wait belongs to the record and goes with it.
                    self.database.clear_file_pending(subject).await?;
                    self.waits.forget(retry);
                }
                Some(reason) => {
                    self.record_download_failure(subject, retry, reason, waits.get(&retry))
                        .await?
                }
            }
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
            if !self.waits.due(retry, waits.get(&retry)) {
                continue;
            }
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
                    self.database.clear_file_pending(subject).await?;
                    self.waits.forget(retry);
                }
                Err(FileReadError::Database(
                    DbError::FileRefChanged { .. } | DbError::FileAbsent { .. },
                )) => self.database.settle_pin_request(file).await?,
                Err(error) => {
                    let reason = self.read_pending(&file, &error);
                    if !retries(&reason) {
                        self.database.settle_pin_request(file).await?;
                    }
                    self.record_download_failure(subject, retry, reason, waits.get(&retry))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Persist the failed download's record with its next wait before it
    /// waits (§16.5): temporary conditions retry once the wait is due,
    /// others wait for the app or an update and keep no wait. A wait that
    /// cannot be chosen or persisted holds the download until the app acts,
    /// and fails the fill.
    async fn record_download_failure(
        &self,
        subject: PendingSubject,
        retry: RetrySubject,
        reason: PendingReason,
        previous: Option<&RetryWait>,
    ) -> Result<(), crate::SyncError> {
        let timed = retries(&reason);
        let wait = if timed {
            match self.waits.next(previous) {
                Ok(wait) => Some(wait),
                Err(error) => {
                    self.waits.hold(retry);
                    return Err(error);
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
        if let Err(error) = recorded {
            self.waits.hold(retry);
            return Err(error.into());
        }
        match wait {
            Some(wait) => self.waits.arm(retry, &wait),
            None => self.waits.forget(retry),
        }
        Ok(())
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

/// Temporary conditions retry on a later pass; a permanent provider
/// failure, refusals of immutable bytes, a missing file and a required
/// update wait for app action or an update (§16.3; io `Downloads.automatic`).
fn retries(reason: &PendingReason) -> bool {
    match reason {
        PendingReason::Storage { failure, .. } => crate::pending::automatic_storage(*failure),
        PendingReason::Refused(_)
        | PendingReason::FileUnavailable(_)
        | PendingReason::UpdateRequired(_) => false,
        _ => true,
    }
}
