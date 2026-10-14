//! The persistent upload queue and its bounded-memory transfer state machine.

use crate::{
    files::file_error::*,
    files::{Files, FilesInner},
    SyncError,
};
use coven_crypto::{FileKey, SecretBytes, FILE_CHUNK_TAG_LEN};
use coven_database::{DbError, FileRef, FileUpload, LocalFileStream};
use coven_format::{
    chunks::CHUNK_SIZE,
    file::{FileHeader, FILE_HEADER_LEN},
};
use coven_storage::{ObjectPath, UploadSession};
use futures_util::StreamExt;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Observable upload queue state (E7).
pub struct UploadQueue {
    /// Whether transfer work is paused.
    pub paused: bool,
    /// Waiting files, oldest first.
    pub files: Vec<QueuedUpload>,
}
/// One waiting upload and its latest progress.
pub struct QueuedUpload {
    /// Captured row version.
    pub file: FileRef,
    /// Current transfer phase.
    pub phase: UploadPhase,
    /// Failed attempts.
    pub attempts: u64,
    /// Latest failure, retaining the native cause while this process lives.
    pub last_failure: Option<Arc<UploadFailure>>,
    /// Enqueue time.
    pub queued_at: SystemTime,
    /// Start of latest attempt.
    pub last_attempt_at: Option<SystemTime>,
}
/// Progress is advanced only over prepared or provider-confirmed bytes.
#[derive(Clone, Debug)]
pub enum UploadPhase {
    /// Not running, or in backoff.
    Waiting,
    /// Checking the source before sending any file bytes.
    Preparing {
        /// Plaintext bytes checked.
        bytes_read: u64,
        /// Whole plaintext size.
        bytes_total: u64,
    },
    /// Verifying chunks, encrypting them and sending the matching ciphertext.
    Uploading {
        /// Provider-confirmed ciphertext bytes.
        bytes_sent: u64,
        /// Whole ciphertext size.
        bytes_total: u64,
    },
}
/// Result of one explicit upload drain.
pub enum DrainOutcome {
    /// Every eligible item was attempted.
    Drained {
        /// Files storage confirmed, each retired from the queue.
        uploaded: usize,
        /// Failed attempts and their causes.
        failures: UploadFailures,
    },
    /// No waiting upload exists.
    QueueEmpty,
    /// Every waiting item is in backoff.
    AllInBackoff,
    /// Uploads were paused.
    Paused,
}
/// Live upload results, ending when the store closes.
pub struct UploadsLiveQuery {
    files: Files,
    changes: tokio::sync::watch::Receiver<u64>,
    first: bool,
    commits: coven_database::DatabaseChanges,
}
impl UploadsLiveQuery {
    /// Current queue immediately, then each change.
    pub async fn next(&mut self) -> Result<UploadQueue, DbError> {
        if !self.first {
            tokio::select! {
                result=self.changes.changed()=>result.map_err(|_|DbError::StoreClosed)?,
                result=self.commits.next()=>result?,
            }
        }
        self.first = false;
        self.changes.borrow_and_update();
        self.files.inner.check_open()?;
        let uploads = self.files.inner.database.uploads().await?;
        let state = self
            .files
            .inner
            .state
            .lock()
            .expect("upload state poisoned");
        let mut files = Vec::new();
        for upload in uploads {
            let last_failure = match state.failures.get(&upload.id) {
                Some(error) => Some(error.clone()),
                None => upload
                    .failure
                    .as_ref()
                    .map(|bytes| {
                        serde_json::from_slice(bytes.as_bytes())
                            .map(|value| Arc::new(UploadFailure::Recorded(value)))
                            .map_err(|_| DbError::DamagedDatabase)
                    })
                    .transpose()?,
            };
            let phase = match state.active.get(&upload.id) {
                Some(phase) => phase.clone(),
                None => UploadPhase::Waiting,
            };
            files.push(QueuedUpload {
                file: upload.file,
                phase,
                attempts: upload.attempts,
                last_failure,
                queued_at: upload.queued_at,
                last_attempt_at: upload.last_attempt_at,
            });
        }
        Ok(UploadQueue {
            paused: state.paused,
            files,
        })
    }
}
impl Files {
    /// Observe queue and transfer changes without polling.
    pub fn subscribe_uploads(&self) -> UploadsLiveQuery {
        UploadsLiveQuery {
            files: self.clone(),
            changes: self.inner.changed.subscribe(),
            first: true,
            commits: self.inner.database.changes(),
        }
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
        if self.paused() {
            return Ok(DrainOutcome::Paused);
        }
        let waiting = self.database.uploads().await?;
        if waiting.is_empty() {
            return Ok(DrainOutcome::QueueEmpty);
        }
        if !self.send.can_send() {
            return Err(coven_storage::StorageFailure::CatchUpRequired.into());
        }
        let mut eligible = Vec::new();
        for item in waiting {
            if force || !in_backoff(&item, self.clock.now())? {
                eligible.push(item);
            }
        }
        if eligible.is_empty() {
            return Ok(DrainOutcome::AllInBackoff);
        }
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
        let mut failures = Vec::new();
        while let Some(attempt) = attempts.next().await {
            match attempt? {
                AttemptOutcome::Stored => uploaded += 1,
                AttemptOutcome::Unchanged => (),
                AttemptOutcome::Failed(file, error) => failures.push((file, error)),
            }
        }
        Ok(DrainOutcome::Drained { uploaded, failures })
    }

    async fn attempt_upload(&self, item: FileUpload) -> Result<AttemptOutcome, DbError> {
        if self.paused() {
            return Ok(AttemptOutcome::Unchanged);
        }
        let _activity = UploadActivity {
            owner: self,
            id: item.id,
        };
        self.database
            .begin_attempt(item.id, self.clock.now())
            .await?;
        match self.upload(&item).await {
            Ok(true) => Ok(AttemptOutcome::Stored),
            Ok(false) => Ok(AttemptOutcome::Unchanged),
            Err(UploadFailure::File(FileReadError::Database(
                error @ DbError::FileCleanup { write: Ok(()), .. },
            ))) => Err(error),
            Err(error) => {
                let bytes =
                    serde_json::to_vec(&error.recording()).map_err(|_| DbError::DamagedDatabase)?;
                self.database
                    .fail_upload(item.id, SecretBytes::new(bytes))
                    .await?;
                let error = Arc::new(error);
                self.state
                    .lock()
                    .expect("upload state poisoned")
                    .failures
                    .insert(item.id, error.clone());
                Ok(AttemptOutcome::Failed(item.file, error))
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
        self.phase(
            item.id,
            UploadPhase::Preparing {
                bytes_read: 0,
                bytes_total: item.file.plaintext_size(),
            },
        );
        // Only this device's queued files have a local source; the queue row
        // being read here means this one does.
        let source = self
            .database
            .open_local(&item.file)
            .await?
            .ok_or(FileReadError::Database(DbError::DamagedDatabase))?;
        let total = FileHeader::new(item.file.plaintext_size())
            .encrypted_size()
            .map_err(|_| FileReadError::Integrity {
                id: item.file.id().to_string(),
            })?;
        self.phase(
            item.id,
            UploadPhase::Preparing {
                bytes_read: item.file.plaintext_size(),
                bytes_total: item.file.plaintext_size(),
            },
        );
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
            self.phase(
                item.id,
                UploadPhase::Uploading {
                    bytes_sent: 0,
                    bytes_total: total,
                },
            );
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
            .complete_upload(item.id)
            .await
            .map_err(FileReadError::from)?;
        self.state
            .lock()
            .expect("upload state poisoned")
            .failures
            .remove(&item.id);
        Ok(true)
    }
    /// A file path is this device's own: different occupant bytes mean another
    /// copy holds its id, which stops every send until the store resets (§10).
    async fn settle_occupied(
        &self,
        storage: &dyn coven_storage::Storage,
        path: &coven_storage::ObjectPath,
        expected: (u64, coven_crypto::ObjectDigest),
    ) -> Result<(), UploadFailure> {
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

enum AttemptOutcome {
    Stored,
    Unchanged,
    Failed(FileRef, Arc<UploadFailure>),
}
struct UploadActivity<'a> {
    owner: &'a FilesInner,
    id: i64,
}
impl Drop for UploadActivity<'_> {
    fn drop(&mut self) {
        self.owner
            .state
            .lock()
            .expect("upload state poisoned")
            .active
            .remove(&self.id);
        self.owner.notify();
    }
}
fn in_backoff(item: &FileUpload, now: SystemTime) -> Result<bool, DbError> {
    let Some(failure) = &item.failure else {
        return Ok(false);
    };
    let failure: RecordedUploadFailure =
        serde_json::from_slice(failure.as_bytes()).map_err(|_| DbError::DamagedDatabase)?;
    if matches!(
        failure,
        RecordedUploadFailure::Storage(coven_storage::StorageFailure::CatchUpRequired)
    ) {
        return Ok(false);
    }
    let Some(last) = item.last_attempt_at else {
        return Ok(false);
    };
    let seconds = (1u64 << item.attempts.saturating_sub(1).min(9)).min(300);
    Ok(match now.duration_since(last) {
        Ok(elapsed) => elapsed < Duration::from_secs(seconds),
        Err(_) => true,
    })
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
                .upload_chunk_hash(self.item.id, index)
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
        self.owner.phase(
            self.item.id,
            UploadPhase::Uploading {
                bytes_sent: offset,
                bytes_total: self.total,
            },
        );
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
