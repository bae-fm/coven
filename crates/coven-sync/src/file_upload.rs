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
use coven_foundation::id_source::FileId;
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
    /// Provider publication is confirmed.
    Stored,
}
/// Result of one explicit upload drain.
pub enum DrainOutcome {
    /// Every eligible item was attempted.
    Drained {
        /// Rows marked uploaded.
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
        for upload in uploads.into_iter().filter(|u| !u.unused) {
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
                None if upload.stored => UploadPhase::Stored,
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
        let uploads = self.database.uploads().await?;
        let waiting = uploads
            .into_iter()
            .filter(|u| !u.unused)
            .collect::<Vec<_>>();
        if waiting.is_empty() {
            return Ok(DrainOutcome::QueueEmpty);
        }
        let eligible = waiting
            .into_iter()
            .filter(|item| force || !in_backoff(item, self.clock.now()))
            .collect::<Vec<_>>();
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

    async fn attempt_upload(&self, mut item: FileUpload) -> Result<AttemptOutcome, DbError> {
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
        match self.upload(&mut item).await {
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
    pub(super) async fn upload(&self, item: &mut FileUpload) -> Result<bool, UploadFailure> {
        if item.identity.is_none() {
            self.fix_identity(item).await?;
        }
        let identity = item.identity.as_ref().expect("recorded identity");
        let (id, key) = decode_identity(identity.as_bytes()).map_err(FileReadError::from)?;
        let device = upload_device(item)?;
        let path = ObjectPath::file(device, id);
        if !item.stored {
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
            let source = self.database.open_local(&item.file).await?;
            let total = FileHeader::new(item.file.plaintext_size())
                .encrypted_size()
                .map_err(|_| FileReadError::Integrity { id: item.file.id() })?;
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
                storage.create_once(&path, &bytes).await?;
            } else {
                let session = item
                    .session
                    .as_ref()
                    .map(|bytes| UploadSession::decode(bytes.as_bytes()))
                    .transpose()?;
                let uploaded = crate::recorded_upload::upload(
                    storage.as_ref(),
                    &path,
                    total,
                    session,
                    &mut transfer,
                )
                .await?;
                if !uploaded {
                    return Ok(false);
                }
            }
            self.database
                .record_stored(item.id)
                .await
                .map_err(FileReadError::from)?;
        }
        self.phase(item.id, UploadPhase::Stored);
        let location = coven_format::file_reference::FileReference { device, id, key }.encode();
        let changed = self
            .database
            .finish_upload(item.id, &item.file, location)
            .await
            .map_err(FileReadError::from)?;
        self.state
            .lock()
            .expect("upload state poisoned")
            .failures
            .remove(&item.id);
        Ok(changed)
    }
    pub(super) async fn fix_identity(&self, item: &mut FileUpload) -> Result<(), UploadFailure> {
        let id = FileId(self.ids.new_id());
        let key = FileKey::generate()?;
        let mut identity = id.to_string().into_bytes();
        identity.extend_from_slice(key.to_secret_bytes().as_bytes());
        self.database
            .record_upload_identity(item.id, SecretBytes::new(identity.clone()))
            .await?;
        item.identity = Some(SecretBytes::new(identity));
        Ok(())
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
pub(crate) fn decode_identity(bytes: &[u8]) -> Result<(FileId, FileKey), DbError> {
    if bytes.len() != 68 {
        return Err(DbError::DamagedDatabase);
    }
    let id = FileId(
        std::str::from_utf8(&bytes[..36])
            .map_err(|_| DbError::DamagedDatabase)?
            .parse()
            .map_err(|_| DbError::DamagedDatabase)?,
    );
    Ok((
        id,
        FileKey::from_bytes(
            bytes[36..]
                .try_into()
                .map_err(|_| DbError::DamagedDatabase)?,
        ),
    ))
}
fn in_backoff(item: &FileUpload, now: SystemTime) -> bool {
    if item.failure.is_none() {
        return false;
    }
    let Some(last) = item.last_attempt_at else {
        return false;
    };
    let seconds = (1u64 << item.attempts.saturating_sub(1).min(9)).min(300);
    match now.duration_since(last) {
        Ok(elapsed) => elapsed < Duration::from_secs(seconds),
        Err(_) => true,
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
                    id: self.item.file.id(),
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

pub(super) fn upload_device(
    item: &FileUpload,
) -> Result<coven_foundation::id_source::DeviceId, UploadFailure> {
    match item.file.location() {
        coven_database::FileLocation::OnDevice(device) => Ok(device),
        coven_database::FileLocation::Uploaded => {
            Err(FileReadError::Database(DbError::DamagedDatabase).into())
        }
    }
}
