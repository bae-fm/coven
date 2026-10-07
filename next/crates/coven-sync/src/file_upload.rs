//! The persistent upload queue and its bounded-memory transfer state machine.

use crate::{
    files::file_error::*,
    files::{join, Files, FilesInner},
    OperationError,
};
use coven_crypto::{ContentHasher, FileKey, SecretBytes};
use coven_database::{DbError, FileRef, FileUpload};
use coven_format::file::FileHeader;
use coven_foundation::{
    files::{FileArea, FileName, FileReader, ObservationError, StoreReadLock},
    id_source::FileId,
};
use coven_storage::{ObjectPath, UploadSession};
use futures_util::StreamExt;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Observable upload queue state (§20.7).
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
    /// Encrypting to durable local bytes.
    Preparing {
        /// Plaintext bytes written.
        bytes_read: u64,
        /// Whole plaintext size.
        bytes_total: u64,
    },
    /// Sending the fixed ciphertext.
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
    /// Record requests and return after the queue commits. Network work never
    /// delays the attaching application write or this durable acceptance.
    pub async fn upload_files(&self, files: &[FileRef]) -> Result<(), OperationError> {
        self.inner.check_open()?;
        for file in files {
            self.inner.database.validate(file).await?;
            if matches!(file.location(), coven_database::FileLocation::OnDevice(_)) {
                self.inner
                    .database
                    .open_local(file)
                    .await
                    .map_err(FileReadError::from)?;
            }
        }
        self.inner
            .database
            .enqueue(files, self.inner.clock.now())
            .await?;
        self.inner.notify();
        self.inner.wake.notify_one();
        Ok(())
    }
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
    pub(crate) async fn drain_uploads(&self, force: bool) -> Result<DrainOutcome, DbError> {
        let _guard = self.drain.lock().await;
        self.check_open()?;
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
        if item.fixed.is_none() {
            self.prepare(item).await?;
        }
        let fixed = item.fixed.as_ref().expect("prepared bytes");
        let (id, key) = decode_identity(fixed.identity.as_bytes()).map_err(FileReadError::from)?;
        let device = upload_device(item)?;
        let path = ObjectPath::file(device, id);
        if !item.stored {
            let storage = self
                .storage
                .read()
                .expect("storage lock poisoned")
                .clone()
                .ok_or(FileReadError::NoStorage)?;
            let file = self.directory.file(FileArea::AppProvided, &fixed.name);
            let row_id = item.file.id();
            let directory = self.directory.clone();
            let read_id = row_id.clone();
            let reader = Arc::new(
                join(tokio::task::spawn_blocking(move || {
                    let lock = directory.lock_read_only()?;
                    let reader = file.open_reader().map_err(|e| spool_error(e, &read_id))?;
                    Ok::<_, FileReadError>(SpoolReader {
                        reader,
                        _lock: lock,
                    })
                }))
                .await?,
            );
            let total = FileHeader::new(item.file.plaintext_size())
                .encrypted_size()
                .map_err(|_| FileReadError::Integrity { id: row_id.clone() })?;
            if reader.size() != total {
                return Err(FileReadError::Integrity { id: row_id }.into());
            }
            // A single request is bounded even for providers accepting GiB bodies.
            if item.session.is_none() && total <= storage.single_request_limit().min(1024 * 1024) {
                let bytes = reader.read_at(0, total as usize, &row_id).await?;
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
                    &mut FileTransfer {
                        owner: self,
                        item,
                        reader,
                        row_id,
                        total,
                    },
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
        let location =
            coven_format::file_reference::UploadedFileReference { device, id, key }.encode();
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
    pub(super) async fn prepare(&self, item: &mut FileUpload) -> Result<(), UploadFailure> {
        let source = self.database.open_local(&item.file).await?;
        let id = FileId(self.ids.new_id());
        let key = FileKey::generate()?;
        let device = upload_device(item)?;
        let path = ObjectPath::file(device, id);
        let name = FileName::new(self.ids.new_id().to_string()).expect("UUID file name");
        let reservation = self.database.reserve_upload_bytes(name.clone()).await?;
        let file = self.directory.file(FileArea::AppProvided, &name);
        let directory = self.directory.clone();
        // Creation retains the reservation until its blocking work finishes,
        // including when the caller cancels while creation is in progress.
        let (reservation, writer) = join(tokio::task::spawn_blocking(move || {
            let writer = (|| -> Result<_, FileReadError> {
                let lock = directory.lock_read_only()?;
                Ok(file.create_writer(lock)?)
            })();
            (reservation, writer)
        }))
        .await;
        let mut writer = writer?;
        let header = FileHeader::new(source.plaintext_size());
        writer.append(&header.encode()).await?;
        let mut hash = ContentHasher::new();
        self.phase(
            item.id,
            UploadPhase::Preparing {
                bytes_read: 0,
                bytes_total: header.size(),
            },
        );
        for index in 0..header.chunk_count() {
            let chunk = header.chunk(index).expect("header bounds");
            let offset = index * u64::from(header.chunk_size());
            let bytes = source
                .read_at(offset, chunk.plaintext_length as u64)
                .await?;
            hash.update(&bytes);
            let sealed = header
                .seal_chunk(&key, path.as_str(), index, &bytes)
                .map_err(|_| FileReadError::Integrity { id: item.file.id() })?;
            writer.append(&sealed).await?;
            self.phase(
                item.id,
                UploadPhase::Preparing {
                    bytes_read: offset + bytes.len() as u64,
                    bytes_total: header.size(),
                },
            );
        }
        if hash.finish() != item.file.content_hash() {
            return Err(FileReadError::Integrity { id: item.file.id() }.into());
        }
        writer.finish().await?;
        let mut identity = id.to_string().into_bytes();
        identity.extend_from_slice(key.to_secret_bytes().as_bytes());
        reservation
            .publish(item.id, SecretBytes::new(identity.clone()))
            .await?;
        item.fixed = Some(coven_database::FixedFileUpload {
            name,
            identity: SecretBytes::new(identity),
        });
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
struct SpoolReader {
    reader: FileReader,
    // Keep the lock inside the value retained by blocking reads after cancellation.
    _lock: StoreReadLock,
}
impl SpoolReader {
    fn size(&self) -> u64 {
        self.reader.size()
    }
    async fn read_at(
        self: &Arc<Self>,
        offset: u64,
        length: usize,
        id: &str,
    ) -> Result<Vec<u8>, FileReadError> {
        let reader = self.clone();
        join(tokio::task::spawn_blocking(move || {
            reader.reader.read_at(offset, length)
        }))
        .await
        .map_err(|error| spool_error(error, id))
    }
}

fn spool_error(error: ObservationError, id: &str) -> FileReadError {
    match error {
        ObservationError::File(error) => FileReadError::Disk(error),
        ObservationError::Missing(_) | ObservationError::Changed(_) => {
            FileReadError::Integrity { id: id.into() }
        }
    }
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
    reader: Arc<SpoolReader>,
    row_id: String,
    total: u64,
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
        Ok(self.reader.read_at(offset, length, &self.row_id).await?)
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
