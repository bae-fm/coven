//! When queued uploads retry (§16.5, E7): transient failures wait their
//! persisted delay, failures that need the app wait for it, and a provider
//! cooldown holds the whole queue.

use crate::{files::file_error::RecordedUploadFailure, files::FilesInner, SyncError};
use coven_crypto::SecretBytes;
use coven_database::{DbError, FileUpload, RetrySubject};
use coven_storage::StorageFailure;

/// Upload failures that retry without the app: transient storage work and
/// a catch-up wait, as their pending records do (§19.1).
pub(super) fn retries_automatically(failure: &RecordedUploadFailure) -> bool {
    match failure {
        RecordedUploadFailure::Storage(StorageFailure::CatchUpRequired) => true,
        RecordedUploadFailure::Storage(failure) => crate::pending::automatic_storage(*failure),
        RecordedUploadFailure::NoStorage
        | RecordedUploadFailure::File
        | RecordedUploadFailure::Local => false,
    }
}

impl FilesInner {
    /// Whether automatic work may attempt the upload now: it has not failed,
    /// or failed with a category that retries automatically and its delay
    /// has elapsed. Other categories wait for the app.
    pub(super) fn retry_due(&self, item: &FileUpload) -> Result<bool, DbError> {
        let automatic = match &item.failure {
            None => true,
            Some(failure) => retries_automatically(
                &serde_json::from_slice(failure.as_bytes())
                    .map_err(|_| DbError::DamagedDatabase)?,
            ),
        };
        Ok(automatic
            && self
                .waits
                .due(RetrySubject::Upload(item.id), item.retry.as_ref()))
    }

    /// When the worker must next drain without other wakes: the earliest
    /// due upload retry, or the end of a provider cooldown holding the queue.
    pub(crate) fn next_retry(&self) -> Option<crate::retry_waits::Wake> {
        // Resuming, connecting and a completed catch-up wake the worker themselves.
        let idle = self.paused()
            || self
                .storage
                .read()
                .expect("storage lock poisoned")
                .is_none()
            || !self.send.can_send();
        if idle {
            return None;
        }
        self.waits
            .next_wake(|subject| matches!(subject, RetrySubject::Upload(_)))
            .or_else(|| self.waits.cooldown_wake())
    }

    /// Record a failed attempt with its next wait, persisting T and W with
    /// the failure before the upload waits. A catch-up wait is woken by the
    /// send gate, and a failure needing the app keeps no timer. A wait that
    /// cannot be persisted holds the upload until the app retries.
    pub(super) async fn record_failure(
        &self,
        item: &FileUpload,
        recording: RecordedUploadFailure,
    ) -> Result<(), SyncError> {
        let subject = RetrySubject::Upload(item.id);
        let bytes = serde_json::to_vec(&recording).map_err(|_| DbError::DamagedDatabase)?;
        let timed = retries_automatically(&recording)
            && !matches!(
                recording,
                RecordedUploadFailure::Storage(StorageFailure::CatchUpRequired)
            );
        let retry = if timed {
            match self.waits.next(item.retry.as_ref()) {
                Ok(wait) => Some(wait),
                Err(next) => {
                    self.waits.hold(subject);
                    return Err(next);
                }
            }
        } else {
            None
        };
        if let Err(persist) = self
            .database
            .fail_upload(item.id, SecretBytes::new(bytes), retry)
            .await
        {
            self.waits.hold(subject);
            return Err(persist.into());
        }
        match &retry {
            Some(wait) => self.waits.arm(subject, wait),
            None => self.waits.forget(subject),
        }
        Ok(())
    }
}
