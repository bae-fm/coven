//! Resolve a checked row reference to its one declared local byte source.

use super::FileRef;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::{
    files::{
        FileArea, FileError, FileName, FileReader, ObservationError, StoreDir, StoreLockError,
        StoreReadLock,
    },
    id_source::DeviceId,
};
use std::{path::PathBuf, sync::Arc};

/// A local file operation's typed failure.
#[derive(Debug, thiserror::Error)]
pub enum LocalFileError {
    /// The recorded original is absent.
    #[error("original {id} is missing at {}", path.display())]
    UserFileMissing {
        /// The file's fixed object id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// The recorded original's facts changed.
    #[error("original {id} changed at {}", path.display())]
    UserFileChanged {
        /// The file's fixed object id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// Bytes did not match the row's content or an owned copy disappeared.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The file's fixed object id.
        id: String,
    },
    /// The requested range exceeds the file's size.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The file's fixed object id.
        id: String,
        /// The requested first byte.
        offset: u64,
        /// The requested exclusive end, saturated if addition overflows.
        end: u64,
        /// The file's whole size.
        size: u64,
    },
    /// Looking up or validating the row failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The filesystem failed.
    #[error(transparent)]
    Disk(#[from] FileError),
    /// The store cannot be retained while opening its file.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
}

/// An open local file checked against one committed row version. Retaining it
/// keeps the opened bytes readable across row replacement and file unlinking,
/// and prevents store deletion until its reads and handles have finished.
pub struct LocalFileStream {
    file: Arc<LocalFileReader>,
}

struct LocalFileReader {
    reader: FileReader,
    // Drop the open file before allowing deletion, even on a cancelled read.
    _lock: StoreReadLock,
    id: String,
    original: Option<PathBuf>,
}

impl LocalFileStream {
    /// The complete plaintext byte count.
    pub fn plaintext_size(&self) -> u64 {
        self.file.plaintext_size()
    }

    /// Read a positioned range on a blocking worker, retaining this open handle.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, LocalFileError> {
        let file = self.file.clone();
        crate::database::finish_blocking(
            tokio::task::spawn_blocking(move || file.read_at(offset, len)).await,
        )
    }

    /// Read a complete plaintext chunk and compare its first-read hash before
    /// returning any bytes to an encryptor. Metadata checks alone cannot detect
    /// a same-size overwrite whose modification time was restored.
    pub async fn read_verified_at(
        &self,
        offset: u64,
        len: u64,
        expected: coven_crypto::ContentHash,
    ) -> Result<Vec<u8>, LocalFileError> {
        let bytes = self.read_at(offset, len).await?;
        let mut hash = coven_crypto::ContentHasher::new();
        hash.update(&bytes);
        if hash.finish() != expected {
            return Err(changed(&self.file.id, self.file.original.as_ref()));
        }
        Ok(bytes)
    }
}

impl LocalFileReader {
    fn plaintext_size(&self) -> u64 {
        self.reader.size()
    }

    #[expect(
        clippy::map_err_ignore,
        reason = "a size past the target integer is TooLarge, which names the field and size"
    )]
    fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, LocalFileError> {
        let end = offset.saturating_add(len);
        if offset.checked_add(len).is_none() || end > self.reader.size() {
            return Err(LocalFileError::RangeOutOfBounds {
                id: self.id.clone(),
                offset,
                end,
                size: self.reader.size(),
            });
        }
        let length = usize::try_from(len).map_err(|_| DbError::TooLarge {
            field: "file range",
            actual: len,
            maximum: usize::MAX as u64,
        })?;
        self.reader
            .read_at(offset, length)
            .map_err(|error| observation(error, &self.id, self.original.is_some()))
    }
}

/// Open the checked local source of a file this device still has queued for
/// upload, after checking the reference against its row. Any other file,
/// including one this device has finished uploading, returns `None`: its bytes
/// are read from the cache or storage (§17.1).
pub(crate) fn open(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    directory: &StoreDir,
    device: DeviceId,
    reference: &FileRef,
) -> Result<Option<LocalFileStream>, LocalFileError> {
    super::validate(db, schema, reference)?;
    if reference.uploader() != device {
        return Ok(None);
    }
    open_queued(db, directory, reference)
}

/// Open a queued file's source from the facts its queue entry fixed at
/// attachment, without consulting the row: a row retargeted, moved or deleted
/// since cannot replace or release them (§17.7).
fn open_queued(
    db: &DatabaseConnection,
    directory: &StoreDir,
    reference: &FileRef,
) -> Result<Option<LocalFileStream>, LocalFileError> {
    queued_source(db, reference)?
        .map(|source| open_source(directory, reference, source))
        .transpose()
}

/// Where a queued file's bytes are: the user's original, as recorded, or
/// coven's own copy.
pub(crate) enum QueuedSource {
    Original {
        path: PathBuf,
        modified_at: std::time::SystemTime,
    },
    Copy(FileName),
}

/// The source a queued file's entry fixed at attachment; `None` once the
/// file is no longer queued. Reads only the database.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn queued_source(
    db: &DatabaseConnection,
    reference: &FileRef,
) -> Result<Option<QueuedSource>, DbError> {
    let sources = db.query(
        "SELECT u.original,u.modified_at,d.path FROM _coven_file_uploads u
         LEFT JOIN _coven_device_files d ON d.device=u.device AND d.file=u.file
         WHERE u.device=?1 AND u.file=?2",
        (
            reference.uploader().0.to_be_bytes(),
            reference.id().0.as_bytes(),
        ),
        |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )?;
    let Some(source) = sources.into_iter().next() else {
        return Ok(None);
    };
    match source {
        (Some(path), Some(modified_at), None) => Ok(Some(QueuedSource::Original {
            path: crate::user_file::decode_path(path)?,
            modified_at: crate::user_file::decode_time(&modified_at)?,
        })),
        (None, None, Some(name)) => Ok(Some(QueuedSource::Copy(
            FileName::new(name).map_err(|_| DbError::DamagedDatabase)?,
        ))),
        _ => Err(DbError::DamagedDatabase),
    }
}

/// Open a queued file's source: it must still have the size, and an
/// original the modification time, recorded at attachment. Reads then return
/// what's on disk; an upload checks each chunk it sends (§17.7). Touches only
/// the disk, never the database.
pub(crate) fn open_source(
    directory: &StoreDir,
    reference: &FileRef,
    source: QueuedSource,
) -> Result<LocalFileStream, LocalFileError> {
    let id = reference.id().to_string();
    let lock = directory.lock_read_only()?;
    let (reader, original) = match source {
        QueuedSource::Original { path, modified_at } => (
            FileReader::open_original(&path, reference.plaintext_size(), modified_at)
                .map_err(|error| observation(error, &id, true))?,
            Some(path),
        ),
        QueuedSource::Copy(name) => {
            let reader = directory
                .file(FileArea::AppProvided, &name)
                .open_reader()
                .map_err(|error| observation(error, &id, false))?;
            // The stream's size is its open handle's, which must be the row's.
            if reader.size() != reference.plaintext_size() {
                return Err(LocalFileError::Integrity { id });
            }
            (reader, None)
        }
    };
    Ok(LocalFileStream {
        file: Arc::new(LocalFileReader {
            reader,
            _lock: lock,
            id,
            original,
        }),
    })
}

fn observation(error: ObservationError, id: &str, original: bool) -> LocalFileError {
    match (error, original) {
        (ObservationError::Missing(path), true) => LocalFileError::UserFileMissing {
            id: id.into(),
            path,
        },
        (ObservationError::Changed(path), true) => LocalFileError::UserFileChanged {
            id: id.into(),
            path,
        },
        (ObservationError::Missing(_) | ObservationError::Changed(_), false) => {
            LocalFileError::Integrity { id: id.into() }
        }
        (ObservationError::File(error), _) => LocalFileError::Disk(error),
    }
}

fn changed(id: &str, original: Option<&PathBuf>) -> LocalFileError {
    match original {
        Some(path) => LocalFileError::UserFileChanged {
            id: id.into(),
            path: path.clone(),
        },
        None => LocalFileError::Integrity { id: id.into() },
    }
}
