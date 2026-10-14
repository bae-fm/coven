//! The persistent upload queue and its bounded-memory transfer state machine.

use crate::{files::file_error::*, files::FilesInner, PendingRecord, PendingSubject, SyncError};
use coven_crypto::{FileKey, FILE_CHUNK_TAG_LEN};
use coven_database::{
    CovenResult, DbError, FileRef, FileUpload, LocalFileStream, RetrySubject, UploadQueueState,
};
use coven_format::{
    chunks::CHUNK_SIZE,
    file::{FileHeader, FILE_HEADER_LEN},
};
use coven_storage::{ObjectPath, UploadSession};
use futures_util::StreamExt;
use std::{sync::atomic::Ordering, time::SystemTime};

/// Observable upload queue state (E7), projected from one committed snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueue {
    /// Whether transfer work is paused; persisted across reopening.
    pub paused: bool,
    /// Waiting files, oldest first.
    pub files: Vec<QueuedUpload>,
}
/// One waiting upload and its committed progress.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedUpload {
    /// Captured row version.
    pub file: FileRef,
    /// Progress the provider confirmed and this device persisted.
    pub phase: UploadPhase,
    /// Enqueue time.
    pub queued_at: SystemTime,
}
/// Progress is advanced only over provider-confirmed, persisted bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadPhase {
    /// Not started, a one-request upload, or waiting for its retry delay.
    Waiting,
    /// Encrypted bytes confirmed by the provider and persisted in the
    /// resumable session. Sending bytes alone does not advance this value.
    Uploading {
        /// Provider-confirmed ciphertext bytes.
        bytes_confirmed: u64,
        /// Whole ciphertext size.
        bytes_total: u64,
    },
}
/// Result of one explicit upload drain. Individual failures stay in
/// `pending()`.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every eligible item was attempted; this many were stored and retired.
    Drained {
        /// Files storage confirmed, each retired from the queue.
        uploaded: usize,
    },
    /// No waiting upload exists.
    QueueEmpty,
    /// Uploads were paused.
    Paused,
}
impl UploadQueue {
    /// Project the committed queue: a recorded provider session's confirmed
    /// bytes are progress, and a file without one is waiting (E7).
    pub fn from_state(state: UploadQueueState) -> CovenResult<Self> {
        let files = state
            .uploads
            .into_iter()
            .map(|upload| {
                let phase = match &upload.session {
                    None => UploadPhase::Waiting,
                    Some(bytes) => {
                        let session = UploadSession::decode(bytes.as_bytes())
                            .map_err(|_| DbError::DamagedDatabase)?;
                        UploadPhase::Uploading {
                            bytes_confirmed: session.confirmed_bytes(),
                            bytes_total: session.total_bytes(),
                        }
                    }
                };
                Ok(QueuedUpload {
                    file: upload.file,
                    phase,
                    queued_at: upload.queued_at,
                })
            })
            .collect::<CovenResult<_>>()?;
        Ok(Self {
            paused: state.paused,
            files,
        })
    }
}
fn file_subject(file: &FileRef) -> PendingSubject {
    PendingSubject::File {
        device: file.uploader(),
        file: file.id(),
    }
}
impl FilesInner {
    pub(crate) async fn drain_uploads(&self, force: bool) -> Result<DrainOutcome, SyncError> {
        let _guard = self.drain.lock().await;
        self.check_open()?;
        // set_storage holds the same guard, so this connection remains in place
        // throughout the drain. Disconnected files wait without failed attempts.
        if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return Err(SyncError::NoStorage);
        }
        // The persisted setting governs; parts check this copy between sends.
        let paused = self.database.uploads_paused().await?;
        self.paused.store(paused, Ordering::Release);
        if paused {
            return Ok(DrainOutcome::Paused);
        }
        let waiting = self
            .database
            .upload_work::<PendingRecord>(file_subject)
            .await?;
        if waiting.is_empty() {
            return Ok(DrainOutcome::QueueEmpty);
        }
        if !self.send.can_send() {
            return Err(coven_storage::StorageFailure::CatchUpRequired.into());
        }
        // An app retry overrides ordinary backoff, never a provider cooldown.
        if let Some(remaining) = self.waits.cooldown() {
            if force {
                return Err(coven_storage::StorageFailure::RateLimited
                    .with_source(coven_storage::CoolingDown { remaining })
                    .into());
            }
            return Ok(DrainOutcome::Drained { uploaded: 0 });
        }
        let eligible: Vec<_> = waiting
            .into_iter()
            .filter(|(item, record)| force || self.retry_due(item, record.as_ref()))
            .map(|(item, _)| item)
            .collect();
        let limit = self
            .limits
            .read()
            .expect("transfer limits lock poisoned")
            .uploads
            .get();
        let mut attempts = futures_util::stream::iter(eligible)
            .map(|item| self.attempt_upload(item))
            .buffer_unordered(limit);
        let mut uploaded = 0;
        while let Some(attempt) = attempts.next().await {
            if attempt? {
                uploaded += 1;
            }
        }
        Ok(DrainOutcome::Drained { uploaded })
    }

    /// Whether this attempt stored the file. A failure becomes the file's
    /// pending record; its native cause stays in this process's log.
    async fn attempt_upload(&self, item: FileUpload) -> Result<bool, SyncError> {
        if self.paused() {
            return Ok(false);
        }
        match self.upload(&item).await {
            Ok(stored) => {
                if stored {
                    self.waits.forget(RetrySubject::Upload(item.id));
                }
                Ok(stored)
            }
            Err(UploadFailure::File(FileReadError::Database(
                error @ DbError::FileCleanup { write: Ok(()), .. },
            ))) => Err(error.into()),
            Err(error) => {
                tracing::debug!(?error, file = %item.file.id(), "file upload attempt failed");
                self.record_failure(&item, &error).await?;
                Ok(false)
            }
        }
    }
    /// Send the file to the path and key its attaching write fixed. Confirmed
    /// storage retires the queue row and changes no app row (§16.5). Returns
    /// false when paused between parts.
    pub(super) async fn upload(&self, item: &FileUpload) -> Result<bool, UploadFailure> {
        let coven_format::file_reference::FileReference { device, id, key } =
            item.file.reference().map_err(FileReadError::from)?;
        let path = ObjectPath::file(self.directory.id(), device, id);
        let storage = self
            .storage
            .read()
            .expect("storage lock poisoned")
            .clone()
            .ok_or(FileReadError::NoStorage)?;
        // The queue fixed this source at attachment; the row may have been
        // retargeted, moved or deleted since without affecting it (§16.5).
        let source = self
            .database
            .open_upload_source(&item.file)
            .await?
            .ok_or(FileReadError::Database(DbError::DamagedDatabase))?;
        let total = FileHeader::new(item.file.plaintext_size())
            .encrypted_size()
            .map_err(|_| FileReadError::Integrity {
                id: item.file.id().to_string(),
            })?;
        let mut transfer = FileTransfer {
            owner: self,
            item,
            source,
            key: &key,
            path: &path,
            total,
        };
        // A single request is bounded even for providers accepting GiB bodies.
        if item.session.is_none() && total <= storage.single_request_limit().min(1024 * 1024) {
            let bytes = transfer.read_encrypted(0, total as usize).await?;
            match storage.create(&path, &bytes).await {
                Ok(()) => {}
                Err(error)
                    if error.failure() == coven_storage::StorageFailure::AlreadyExists
                        && !matches!(error, coven_storage::StorageError::Cleanup { .. }) =>
                {
                    self.settle_occupied(storage.as_ref(), &path, crate::identity::digest(&bytes))
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            let session = item
                .session
                .as_ref()
                .map(|bytes| UploadSession::decode(bytes.as_bytes()))
                .transpose()?;
            match crate::recorded_upload::upload(
                storage.as_ref(),
                &path,
                total,
                session,
                &mut transfer,
            )
            .await?
            {
                crate::recorded_upload::Uploaded::Stored => {}
                crate::recorded_upload::Uploaded::Paused => return Ok(false),
                crate::recorded_upload::Uploaded::Occupied(expected) => {
                    self.settle_occupied(storage.as_ref(), &path, expected)
                        .await?;
                }
            }
        }
        self.database
            .complete_upload(item.id, file_subject(&item.file))
            .await
            .map_err(FileReadError::from)?;
        Ok(true)
    }
    /// A file path is this device's own: different occupant bytes mean another
    /// copy holds its id, which stops every send until the store resets (§10).
    /// The comparison read is an upload-worker request, so it passes the same
    /// fresh catch-up check as a send (SP, file uploads outside the pass).
    async fn settle_occupied(
        &self,
        storage: &dyn coven_storage::Storage,
        path: &coven_storage::ObjectPath,
        expected: (u64, coven_crypto::ObjectDigest),
    ) -> Result<(), UploadFailure> {
        coven_storage::PublicationGate::check(self.send.as_ref(), path)?;
        match crate::identity::settle_occupied(storage, &self.send, path, expected).await {
            Ok(()) => Ok(()),
            Err(SyncError::Storage(error)) => Err(error.into()),
            Err(SyncError::DeviceReset(_)) => Err(coven_storage::StorageFailure::AlreadyExists
                .with_source("another copy of this device published different bytes")
                .into()),
            Err(error) => Err(coven_storage::StorageFailure::Protocol
                .with_source(error)
                .into()),
        }
    }
}

struct FileTransfer<'a> {
    owner: &'a FilesInner,
    item: &'a FileUpload,
    source: LocalFileStream,
    key: &'a FileKey,
    path: &'a ObjectPath,
    total: u64,
}
impl FileTransfer<'_> {
    async fn read_encrypted(&self, offset: u64, length: usize) -> Result<Vec<u8>, UploadFailure> {
        let header = FileHeader::new(self.item.file.plaintext_size());
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= self.total)
            .ok_or(DbError::DamagedDatabase)?;
        let mut bytes = Vec::with_capacity(length);
        let mut position = offset;
        if position < FILE_HEADER_LEN as u64 {
            let next = end.min(FILE_HEADER_LEN as u64);
            bytes.extend_from_slice(&header.encode()[position as usize..next as usize]);
            position = next;
        }
        while position < end {
            let index =
                (position - FILE_HEADER_LEN as u64) / (CHUNK_SIZE + FILE_CHUNK_TAG_LEN) as u64;
            let chunk = header.chunk(index).map_err(|_| DbError::DamagedDatabase)?;
            let expected = self
                .owner
                .database
                .upload_chunk_hash(&self.item.file, index)
                .await?;
            let plain = self
                .source
                .read_verified_at(
                    index * CHUNK_SIZE as u64,
                    chunk.plaintext_length as u64,
                    expected,
                )
                .await?;
            // The exact buffer checked above is the only input to encryption.
            // Re-reading after checking would permit a source change between them.
            let sealed = header
                .seal_chunk(self.key, self.path.as_str(), index, &plain)
                .map_err(|_| FileReadError::Integrity {
                    id: self.item.file.id().to_string(),
                })?;
            let start = (position - chunk.offset) as usize;
            let count = (end - position).min((sealed.len() - start) as u64) as usize;
            bytes.extend_from_slice(&sealed[start..start + count]);
            position += count as u64;
        }
        Ok(bytes)
    }
}
impl crate::recorded_upload::UploadSource for FileTransfer<'_> {
    type Error = UploadFailure;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, UploadFailure> {
        self.read_encrypted(offset, length).await
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), UploadFailure> {
        self.owner
            .database
            .record_session(self.item.id, session.encode()?)
            .await?;
        Ok(())
    }
    fn keep_going(&self) -> bool {
        !self.owner.paused()
    }
}
