//! One attempt to upload a queued file, through its bounded-memory transfer
//! state machine.

use crate::{
    files::file_error::*, files::file_failure::file_subject, files::FilesInner, SyncError,
};
use coven_crypto::{FileKey, FILE_CHUNK_TAG_LEN};
use coven_database::FileRefJournal;
use coven_database::TransferMeter;
use coven_database::{DbError, FileUpload, LocalFileStream, RetrySubject};
use coven_format::{
    chunks::CHUNK_SIZE,
    file::{FileHeader, FILE_HEADER_LEN},
};
use coven_storage::{ObjectPath, SendProgress, UploadSession};

#[path = "file_upload_copy.rs"]
mod copy;

impl FilesInner {
    /// Whether this attempt stored the file. A failure becomes the file's
    /// pending record, which holds its error as the cause in this process.
    pub(super) async fn attempt_upload(
        &self,
        item: FileUpload,
        meter: TransferMeter,
    ) -> Result<bool, SyncError> {
        if self.paused() {
            return Ok(false);
        }
        self.forget_connection_wait(RetrySubject::Upload(item.id));
        match self.upload(&item, meter).await {
            Ok(stored) => {
                if stored {
                    self.waits.forget(RetrySubject::Upload(item.id));
                    self.waits
                        .causes()
                        .cleared(&file_subject(&item.file), |_| true);
                }
                Ok(stored)
            }
            Err(error) => {
                self.record_failure(&item, error).await?;
                Ok(false)
            }
        }
    }
    /// Send the file to the path and key its attaching write fixed, telling
    /// `meter` how far through the encrypted object the bytes handed over
    /// reach. Confirmed storage retires the queue row and changes no app row
    /// (§17.7). Returns false when paused between parts.
    pub(super) async fn upload(
        &self,
        item: &FileUpload,
        meter: TransferMeter,
    ) -> Result<bool, TransferError> {
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
        // retargeted, moved or deleted since without affecting it (§17.7).
        let source =
            self.database
                .open_upload_source(&item.file)
                .await?
                .ok_or(FileReadError::Database(DbError::Invariant {
                    what: "a queued upload keeps the source fixed at attachment",
                }))?;
        let total = item.bytes_total;
        let sent = {
            let meter = meter.clone();
            SendProgress::new(move |bytes| meter.reached(bytes))
        };
        let mut transfer = FileTransfer {
            owner: self,
            item,
            source,
            key: &key,
            path: &path,
            total,
            meter,
            sent,
        };
        // One request within the provider's limit holds the whole encrypted
        // file; past the buffered bound a session holds one part at a time.
        let sent = if item.session.is_none()
            && total <= coven_storage::buffered_request_limit(storage.as_ref())
        {
            let bytes = transfer.read_encrypted(0, total as usize).await?;
            match storage
                .create_reporting(&path, &bytes, &transfer.sent)
                .await
            {
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
            Some(bytes)
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
                crate::recorded_upload::Uploaded::Paused => return Ok(false),
                crate::recorded_upload::Uploaded::Occupied(expected) => {
                    self.settle_occupied(storage.as_ref(), &path, expected)
                        .await?;
                }
            }
            None
        };
        transfer.complete(sent).await?;
        Ok(true)
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
    /// This attempt's progress: how far each recorded session has got.
    meter: TransferMeter,
    /// Hears each request's bytes as they are handed over.
    sent: SendProgress,
}
impl FileTransfer<'_> {
    #[expect(
        clippy::map_err_ignore,
        reason = "a value past the target integer breaks the named invariant"
    )]
    async fn read_encrypted(&self, offset: u64, length: usize) -> Result<Vec<u8>, TransferError> {
        let header = FileHeader::new(self.item.file.plaintext_size());
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= self.total)
            .ok_or(DbError::Invariant {
                what: "an upload part lies inside the encrypted file",
            })?;
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
            let chunk = header.chunk(index).map_err(|_| DbError::Invariant {
                what: "an upload part's chunks exist in the file",
            })?;
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
                .map_err(|error| FileReadError::chunk(self.item.file.id().to_string(), error))?;
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
        self.owner
            .database
            .record_session(self.item.id, session.encode()?, progress)
            .await?;
        // A resumed session is as far as the provider says; one started
        // again is back at its start.
        self.meter.reached(session.confirmed_bytes());
        Ok(())
    }
    fn sent(&self) -> SendProgress {
        self.sent.clone()
    }
    fn keep_going(&self) -> bool {
        !self.owner.paused() && self.owner.send.files_allowed()
    }
}
