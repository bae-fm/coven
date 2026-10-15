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
    /// budgets. Progress is the committed coverage `pin_progress` reads.
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let limit = self.transfer_limits().downloads.get();
        futures_util::stream::iter(files.to_vec())
            .map(|file| async move {
                // A file still queued here is already whole on this device.
                if self.inner.database.open_local(&file).await?.is_none()
                    && !self.inner.database.pin_complete(&file).await?
                {
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
    /// Unconfigured namespaces have no eviction budget.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
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
    pub(crate) async fn fill_eager(self: &Arc<Self>) {
        let _fill = self.eager_fill.lock().await;
        if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return;
        }
        let files = match self.database.eager_work().await {
            Ok(files) => files,
            Err(error) => {
                // No file subject holds this failure; the next wake retries it.
                tracing::error!(?error, "reading eager work failed");
                return;
            }
        };
        if files.is_empty() {
            return;
        }
        let waits: std::collections::BTreeMap<_, _> = match self.database.download_waits().await {
            Ok(waits) => waits.into_iter().collect(),
            Err(error) => {
                // No file subject holds this failure; the next wake retries it.
                tracing::error!(?error, "reading eager download waits failed");
                return;
            }
        };
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
            return;
        }
        // The reservations are the fill's progress and cancellation (§16.4).
        if let Err(error) = self.database.request_eager(pending.clone()).await {
            tracing::error!(?error, "reserving eager downloads failed");
            return;
        }
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
                        if let Err(error) = self.waits.clear(download_subject(file)).await {
                            tracing::error!(?error, "ending a cancelled download's wait failed");
                        }
                    }
                    if let Err(error) = self.database.settle_eager_work(pending.clone()).await {
                        tracing::error!(?error, "settling cancelled eager work failed");
                    }
                    return;
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
                    if let Err(error) = self.database.evict(file).await {
                        tracing::error!(?error, "evicting a refused eager download failed");
                    }
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
                if let Err(error) = self.database.settle_eager_work(vec![file.clone()]).await {
                    tracing::error!(?error, "settling eager work failed");
                }
            }
            match reason {
                None => {
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
        // The set retires together once every file in it is cached.
        if let Err(error) = self.database.retire_eager().await {
            tracing::error!(?error, "retiring completed eager downloads failed");
        }
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
