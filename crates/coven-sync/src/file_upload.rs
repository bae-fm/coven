//! One attempt to upload a queued file, through its bounded-memory transfer
//! state machine.

use crate::{
    files::file_error::*, files::file_failure::upload_subject, files::FilesInner, SyncError,
};
use coven_crypto::FileKey;
use coven_database::FileLocationKey;
use coven_database::TransferMeter;
use coven_database::{DbError, FileUpload, LocalFileStream, RetrySubject};
use coven_format::file::{FileHeader, FILE_HEADER_LEN};
use coven_storage::{ByteProgress, ObjectPath, UploadSession};

/// How an upload attempt ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Attempted {
    /// Storage holds the bytes, and the files waiting for them switch with
    /// their move.
    Stored,
    /// Storage holds the bytes, but no file waited for them any more: the
    /// upload retired, storing no file (§17.3).
    Retired,
    /// The upload stays queued: the attempt failed, or a hold stopped it.
    Queued,
}

impl FilesInner {
    /// How this attempt ended. A failure becomes the file's pending record,
    /// which holds its error as the cause in this process. The pause and a
    /// metered network hold a trailing upload only (§17.4, §17.9).
    pub(super) async fn attempt_upload(
        &self,
        item: FileUpload,
        meter: TransferMeter,
    ) -> Result<Attempted, SyncError> {
        if item.holds.is_none() && self.trailing_held() {
            return Ok(Attempted::Queued);
        }
        self.forget_connection_wait(RetrySubject::Upload(item.id));
        match self.upload(&item, meter).await {
            Ok(attempted) => {
                if attempted != Attempted::Queued {
                    self.waits.forget(RetrySubject::Upload(item.id));
                    self.waits.failure_cleared(&upload_subject(&item.location), |_| true);
                }
                Ok(attempted)
            }
            Err(error) => {
                self.record_failure(&item, error).await?;
                Ok(Attempted::Queued)
            }
        }
    }
    /// Send the file to the path and key its staging write fixed, telling
    /// `meter` how far through the encrypted object the bytes handed over
    /// reach. Confirmed storage switches its files to the cloud and retires
    /// the queue row (§17.9). A trailing upload held between parts, or whose
    /// completion a self-replacement's pause holds, stays queued.
    pub(super) async fn upload(
        &self,
        item: &FileUpload,
        meter: TransferMeter,
    ) -> Result<Attempted, TransferError> {
        let coven_format::file_reference::FileReference { key, .. } =
            item.location.reference().map_err(FileReadError::from)?;
        let path = item.location.path(self.directory.id());
        let storage = self
            .storage
            .read()
            .expect("storage lock poisoned")
            .clone()
            .ok_or(FileReadError::NoStorage)?;
        // The queue fixed this source at staging; the rows naming its files
        // may have changed since without affecting it (§17.9).
        let source =
            self.database.open_upload_source(item).await?.ok_or(FileReadError::Database(
                DbError::Invariant { what: "a queued upload keeps the source fixed at staging" },
            ))?;
        let total = item.bytes_total;
        let sent = {
            let meter = meter.clone();
            ByteProgress::new(move |bytes| meter.reached(bytes))
        };
        let mut transfer =
            FileTransfer { owner: self, item, source, key: &key, path: &path, total, meter, sent };
        // One request within the provider's limit holds the whole encrypted
        // file; past the buffered bound a session holds one part at a time.
        if item.session.is_none()
            && total <= coven_storage::buffered_request_limit(storage.as_ref())
        {
            let bytes = transfer.read_encrypted(0, total as usize).await?;
            match storage.create_reporting(&path, &bytes, &transfer.sent).await {
                Ok(_) => {}
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
                &coven_storage::UploadTarget::Create(path.clone()),
                total,
                session,
                &mut transfer,
            )
            .await?
            {
                crate::recorded_upload::Uploaded::Stored => {}
                crate::recorded_upload::Uploaded::Paused => return Ok(Attempted::Queued),
                crate::recorded_upload::Uploaded::Occupied(expected) => {
                    self.settle_occupied(storage.as_ref(), &path, expected).await?;
                }
            }
        }
        // Storage holds bytes the source gave: whatever was recorded
        // about the source failing ends.
        self.settled_source(item.id, &item.location, item.of).await.map_err(
            |error| match error {
                SyncError::Database(error) => FileReadError::from(error),
                error => FileReadError::Database(DbError::SyncStream(Box::new(error))),
            },
        )?;
        match self.database.complete_upload(item.id, upload_subject(&item.location)).await {
            Ok(coven_database::UploadCompletion::Stored) => Ok(Attempted::Stored),
            Ok(coven_database::UploadCompletion::Retired) => Ok(Attempted::Retired),
            // A self-replacement paused writes while the file was sent. The
            // stored bytes wait under their path, and the upload stays
            // queued, held, until a run after the pause finds them and
            // completes it (§12.3, §17.4).
            Err(DbError::MemberKeysChanging) => {
                self.writes_were_paused();
                Ok(Attempted::Queued)
            }
            Err(error) => Err(FileReadError::from(error).into()),
        }
    }
    /// Different bytes at a file path fail this upload attempt (§19.3).
    async fn settle_occupied(
        &self,
        storage: &dyn coven_storage::Storage,
        path: &coven_storage::ObjectPath,
        expected: (u64, coven_crypto::ObjectDigest),
    ) -> Result<(), TransferError> {
        coven_storage::PublicationGate::check(self.send.as_ref(), path)?;
        match crate::identity::settle_occupied(storage, path, expected).await {
            Ok(()) => Ok(()),
            Err(SyncError::Storage(error)) => Err(error.into()),
            Err(SyncError::Damaged(object)) => Err(TransferError::Refused(object.failure)),
            Err(error) => Err(coven_storage::StorageFailure::Protocol.with_source(error).into()),
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
    /// This attempt's progress: how far each recorded session has got.
    meter: TransferMeter,
    /// Hears each request's bytes as they are handed over.
    sent: ByteProgress,
}
impl FileTransfer<'_> {
    #[expect(
        clippy::map_err_ignore,
        reason = "a value past the target integer breaks the named invariant"
    )]
    async fn read_encrypted(&self, offset: u64, length: usize) -> Result<Vec<u8>, TransferError> {
        let header = FileHeader::new(self.item.size);
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= self.total)
            .ok_or(DbError::Invariant { what: "an upload part lies inside the encrypted file" })?;
        let mut bytes = Vec::with_capacity(length);
        let mut position = offset;
        if position < FILE_HEADER_LEN as u64 {
            let next = end.min(FILE_HEADER_LEN as u64);
            bytes.extend_from_slice(&header.encode()[position as usize..next as usize]);
            position = next;
        }
        while position < end {
            let chunk = header.chunk_at(position).map_err(|_| DbError::Invariant {
                what: "an upload part's chunks exist in the file",
            })?;
            let index = chunk.index;
            let expected =
                self.owner.database.upload_chunk_hash(&self.item.location, index).await?;
            let plain = self
                .source
                .read_verified_at(chunk.plaintext_start(), chunk.plaintext_length as u64, expected)
                .await?;
            // The exact buffer checked above is the only input to encryption.
            // Re-reading after checking would permit a source change between them.
            let sealed = header.seal_chunk(self.key, self.path.as_str(), index, &plain).map_err(
                |error| FileReadError::chunk(self.item.location.object().to_string(), error),
            )?;
            let start = (position - chunk.offset) as usize;
            let count = (end - position).min((sealed.len() - start) as u64) as usize;
            bytes.extend_from_slice(&sealed[start..start + count]);
            position += count as u64;
        }
        Ok(bytes)
    }
}
impl crate::recorded_upload::UploadSource for FileTransfer<'_> {
    type Error = TransferError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, TransferError> {
        self.read_encrypted(offset, length).await
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), TransferError> {
        let progress = crate::UploadProgress {
            bytes_confirmed: session.confirmed_bytes(),
            bytes_total: session.total_bytes(),
        };
        self.owner.database.record_session(self.item.id, session.encode()?, progress).await?;
        // A resumed session is as far as the provider says; one started
        // again is back at its start.
        self.meter.reached(session.confirmed_bytes());
        Ok(())
    }
    fn sent(&self) -> ByteProgress {
        self.sent.clone()
    }
    fn keep_going(&self) -> bool {
        (self.item.holds.is_some() || !self.owner.trailing_held())
            && self.owner.send.files_allowed()
    }
}
