//! The pins coven keeps itself (§17.8): files a switch to the cloud asked
//! this device to keep whole. Unlike the app's own pin, nothing calls again
//! for one, so coven makes it, resuming after a restart, and its failures
//! retry under the shared backoff (§6.7). An app file's cache copy is made
//! from coven's own copy, checked against the hashes its staging recorded,
//! with no request to storage; if that fails, or for a user's file, the pin
//! downloads from storage. A user's original is never read for the cache.

use super::file_error::{FileReadError, TransferError};
use super::file_failure::{stored, FileCopy, FileFailure};
use crate::{
    files::file_read::UploadedFile, files::FilesInner, PendingReason, PendingRecord,
    PendingSubject, Prerequisite, Retry, SyncError,
};
use coven_crypto::ContentHasher;
use coven_database::{DbError, FileLocationKey, FileRef, PinRequest, RetrySubject};
use coven_format::{chunks::CHUNK_SIZE, file::FileHeader};
use coven_foundation::files::FileName;
use coven_storage::ObjectPath;
use std::sync::Arc;

impl FilesInner {
    /// Make each pin coven keeps whose wait is over and whose failure needs
    /// no new connection.
    pub(super) async fn keep_pinned(self: &Arc<Self>) -> Result<(), SyncError> {
        // The upload worker owns these waits: a report it has not acted on
        // ends them before they are judged (§6.7).
        self.waits.end_reported_waits(crate::NetworkOwner::Uploads).await?;
        for request in self.database.pin_requests().await? {
            let subject = RetrySubject::Pin(request.file);
            if !self.waits.due(subject, request.retry.as_ref()) || !self.connection_due(subject) {
                continue;
            }
            let file = match self.database.file_ref(request.file).await {
                Ok(file) => file,
                // Its record went in a write since the read above, which
                // ended the request too.
                Err(DbError::FileAbsent { .. }) => continue,
                Err(error) => return Err(error.into()),
            };
            if stored(&file).is_err() {
                return Err(DbError::Invariant { what: "a file kept here is in the cloud" }.into());
            }
            let _pins = self.pins.lock().await;
            self.check_open()?;
            let outcome = if self.database.pin_complete(&file).await? {
                Ok(())
            } else {
                self.keep(&file, &request).await
            };
            match outcome {
                Ok(()) => self.ended(&file).await?,
                // The file left the bytes while they downloaded: the write
                // taking it off them ended the request, and nothing was kept.
                Err(TransferError::File(FileReadError::Database(DbError::NoLongerStored {
                    ..
                }))) => self.ended(&file).await?,
                Err(error) => self.keep_failed(&request, error).await?,
            }
        }
        Ok(())
    }

    /// Make the cache copy: from coven's copy while the request keeps one,
    /// or else downloaded.
    async fn keep(
        self: &Arc<Self>,
        file: &FileRef,
        request: &PinRequest,
    ) -> Result<(), TransferError> {
        if let Some(copy) = &request.copy {
            match self.keep_from_copy(file, copy).await {
                Ok(()) => return Ok(()),
                Err(
                    error @ TransferError::File(FileReadError::Database(DbError::NoLongerStored {
                        ..
                    })),
                ) => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        file = %file.id(),
                        "keeping a file from coven's copy failed, so it downloads: {error}"
                    );
                    self.database.drop_pin_copy(file.id()).await?;
                }
            }
        }
        UploadedFile::open(self.clone(), file.clone()).await?.keep_whole().await
    }

    /// Write the stored object coven's copy encrypts to, chunk by checked
    /// chunk, as the file's pinned whole cache entry. Its upload sent these
    /// very bytes: the same plaintext under the same key and chunk nonces.
    async fn keep_from_copy(&self, file: &FileRef, copy: &FileName) -> Result<(), TransferError> {
        let (location, content_hash) = stored(file)?;
        let coven_format::file_reference::FileReference { device, id, key } =
            location.reference().map_err(FileReadError::from)?;
        let path = ObjectPath::file(self.directory.id(), device, id);
        let source = self.database.open_pin_copy(file, copy).await.map_err(FileReadError::from)?;
        let header = FileHeader::new(file.size());
        let (reservation, mut writer) = self.create_cache_file(file).await?;
        writer.append(&header.encode()).await?;
        let mut hash = ContentHasher::new();
        for index in 0..header.chunk_count() {
            let chunk = header.chunk(index).map_err(|_outside| FileReadError::Integrity {
                id: file.id().to_string(),
                cause: None,
            })?;
            let expected = self.database.copy_chunk_hash(copy, index).await?;
            let plain = source
                .read_verified_at(
                    index * CHUNK_SIZE as u64,
                    chunk.plaintext_length as u64,
                    expected,
                )
                .await
                .map_err(FileReadError::from)?;
            hash.update(&plain);
            let sealed = header
                .seal_chunk(&key, path.as_str(), index, &plain)
                .map_err(|error| FileReadError::chunk(file.id().to_string(), error))?;
            writer.append(&sealed).await?;
        }
        if hash.finish() != content_hash {
            return Err(TransferError::Refused(crate::Refusal::ContentHash));
        }
        writer.finish().await?;
        let _cache = self.cache.write().await;
        reservation.publish().await?;
        Ok(())
    }

    /// The pin is made, or the file left its stored bytes: its request,
    /// record and wait end.
    async fn ended(&self, file: &FileRef) -> Result<(), SyncError> {
        self.database.end_pin_request(file.id()).await?;
        let subject = RetrySubject::Pin(file.id());
        self.waits.forget(subject);
        self.forget_connection_wait(subject);
        self.waits.causes().cleared(&PendingSubject::Keep { file: file.id() }, |_| true);
        Ok(())
    }

    /// Record a failed attempt as the pin's record, with its next wait when
    /// it clears by itself, persisted before it waits. One a person must
    /// clear waits for a new connection; one that never clears keeps no
    /// timer.
    async fn keep_failed(
        &self,
        request: &PinRequest,
        error: TransferError,
    ) -> Result<(), SyncError> {
        if matches!(&error, TransferError::File(FileReadError::Database(DbError::StoreClosed))) {
            return Ok(());
        }
        let subject = RetrySubject::Pin(request.file);
        let pending = PendingSubject::Keep { file: request.file };
        let reason = self.file_failure(&pending, FileCopy::Stored, FileFailure::Held(error));
        let needs_connection = reason.clears() == Some(crate::pending::Clears::ByPerson);
        let record = PendingRecord::new(pending, reason);
        let timed = record.retry() == Retry::Automatic
            && record.reason != PendingReason::Waits(Prerequisite::SequenceCatchUp);
        let retry = if timed {
            match self.waits.next(request.retry.as_ref()) {
                Ok(wait) => Some(wait),
                Err(next) => {
                    self.waits.hold(subject);
                    return Err(next);
                }
            }
        } else {
            None
        };
        if let Err(persist) = self.database.fail_pin(request.file, record, retry).await {
            self.waits.hold(subject);
            return Err(persist.into());
        }
        if needs_connection {
            self.wait_for_connection(subject);
        }
        match &retry {
            Some(wait) => self.waits.arm(subject, wait),
            None => self.waits.forget(subject),
        }
        Ok(())
    }
}
