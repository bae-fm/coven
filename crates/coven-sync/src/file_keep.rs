//! The pins coven keeps itself (§17.8): files a switch to the cloud asked
//! this device to keep whole. Unlike the app's own pin, nothing calls again
//! for one, so coven makes it, resuming after a restart, and its failures
//! retry under the shared backoff (§6.7). An app file's cache copy is made
//! from coven's own copy, checked against the hashes its staging recorded,
//! with no request to storage; if that fails, or for a user's file, the pin
//! downloads from storage. A user's original is never read for the cache.

use super::file_error::{FileReadError, TransferError};
use super::file_failure::{stored, FileFailure};
use crate::{files::FilesInner, PendingReason, Prerequisite, Retry, SyncError};
use coven_crypto::ContentHasher;
use coven_database::{
    CacheReservation, DbError, FileLocationKey, FileRef, PinRequest, RetrySubject,
};
use coven_format::file::FileHeader;
use coven_foundation::files::FileName;
use coven_storage::ObjectPath;
use std::sync::Arc;

/// What one attempt at a pin coven keeps came to.
enum Kept {
    /// The pin is made, in the write that ended its request.
    Made,
    /// A metered network the app keeps files off held its download (§17.8):
    /// nothing was made, and nothing failed.
    Held,
    /// Its request ended in a write since it was read, by the app's unpin or
    /// the file leaving: nothing was made.
    Ended,
}

impl FilesInner {
    /// Make each pin coven keeps whose wait is over and whose failure needs
    /// no new connection.
    pub(super) async fn keep_pinned(self: &Arc<Self>) -> Result<(), SyncError> {
        // The upload worker owns these waits: a report it has not acted on
        // ends them before they are judged (§6.7).
        self.waits.end_reported_waits(crate::NetworkOwner::Uploads).await?;
        // The persisted setting governs whether a metered network holds
        // their downloads (§17.8).
        self.read_unmetered_only().await?;
        let requests = self.database.pin_requests().await?;
        // A wait names its request, whose id is never reused, so one this
        // session keeps for a request ended since names nothing any more.
        self.retain_pin_waits(&requests.iter().map(|request| request.id).collect());
        for request in requests {
            let subject = RetrySubject::Pin(request.id);
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
            let outcome = match self.pin_kept(&file, &request, true).await {
                // The copy already kept was evicted before the pin's write:
                // the file is kept again from its bytes.
                Err(TransferError::File(FileReadError::Database(DbError::PinChanged {
                    ..
                }))) => self.pin_kept(&file, &request, false).await,
                outcome => outcome,
            };
            match outcome {
                // The write making the pin ended its request, or a write
                // since the read above did.
                Ok(Kept::Made | Kept::Ended) => self.ended_here(&request),
                // A metered network the app keeps files off holds it; the
                // upload worker runs again as the network or the setting
                // changes (§17.8).
                Ok(Kept::Held) => {}
                // The file left the bytes while they downloaded: the write
                // taking it off them ended the request, and nothing was kept.
                Err(TransferError::File(FileReadError::Database(DbError::NoLongerStored {
                    ..
                }))) => self.ended(&request).await?,
                Err(error) => self.keep_failed(&request, error).await?,
            }
        }
        Ok(())
    }

    /// Prepare the file's pin, from a whole copy already kept if `kept`
    /// finds one, and otherwise from its cache copy made here, then make it
    /// in one write that ends its request too, if the request still waits.
    async fn pin_kept(
        self: &Arc<Self>,
        file: &FileRef,
        request: &PinRequest,
        kept: bool,
    ) -> Result<Kept, TransferError> {
        let prepared = if kept && self.database.whole_kept(file).await? {
            self.database.prepared_pin(Vec::new(), vec![file.clone()])?
        } else {
            let Some(copy) = self.keep(file, request).await? else {
                return Ok(Kept::Held);
            };
            self.database.prepared_pin(vec![copy], Vec::new())?
        };
        Ok(if self.database.keep(prepared, request.id).await? { Kept::Made } else { Kept::Ended })
    }

    /// Make the cache copy: from coven's copy while the request keeps one,
    /// or else downloaded; `None` when a metered network held the download.
    async fn keep(
        self: &Arc<Self>,
        file: &FileRef,
        request: &PinRequest,
    ) -> Result<Option<CacheReservation>, TransferError> {
        if let Some(copy) = &request.copy {
            match self.keep_from_copy(file, copy).await {
                Ok(reservation) => return Ok(Some(reservation)),
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
                    self.database.drop_pin_copy(request.id).await?;
                }
            }
        }
        self.download_whole(file.clone()).await
    }

    /// Write the stored object coven's copy encrypts to, chunk by checked
    /// chunk, as the file's pinned whole cache entry. Its upload sent these
    /// very bytes: the same plaintext under the same key and chunk nonces.
    async fn keep_from_copy(
        &self,
        file: &FileRef,
        copy: &FileName,
    ) -> Result<CacheReservation, TransferError> {
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
                .read_verified_at(chunk.plaintext_start(), chunk.plaintext_length as u64, expected)
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
        Ok(reservation)
    }

    /// The file left its stored bytes: its request, record and wait end.
    async fn ended(&self, request: &PinRequest) -> Result<(), SyncError> {
        self.database.end_pin_request(request.id).await?;
        self.ended_here(request);
        Ok(())
    }

    /// The request ended in a write: its wait and logged failure end in
    /// this session too.
    fn ended_here(&self, request: &PinRequest) {
        let subject = RetrySubject::Pin(request.id);
        self.waits.forget(subject);
        self.forget_connection_wait(subject);
        let logged = crate::failure_log::Logged::Kept { request: request.id, file: request.file };
        self.waits.failure_cleared(logged, |_| true);
    }

    /// Let go of the waits and logged failures this session keeps for pin
    /// requests not in `live`, which have ended.
    fn retain_pin_waits(&self, live: &std::collections::BTreeSet<i64>) {
        let ended =
            |subject: &RetrySubject| matches!(subject, RetrySubject::Pin(id) if !live.contains(id));
        self.waits.retain(|subject| !ended(subject));
        self.retain_connection_waits(|subject| !ended(subject));
        self.waits.retain_kept_failures(live);
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
        let subject = RetrySubject::Pin(request.id);
        let record = self.kept_failure(request, FileFailure::Held(error));
        let needs_connection = record.reason.clears() == Some(crate::pending::Clears::ByPerson);
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
        if let Err(persist) = self.database.fail_pin(request, record, retry).await {
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
