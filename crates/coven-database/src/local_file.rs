//! Resolve a checked row reference to its one declared local byte source.

use super::FileRef;
use crate::{file_row, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, Provenance};
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
/// upload. Any other file, including one this device has finished uploading,
/// returns `None`: its bytes are read from the cache or storage (§16.1).
pub(crate) fn open(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    directory: &StoreDir,
    device: DeviceId,
    reference: &FileRef,
) -> Result<Option<LocalFileStream>, LocalFileError> {
    super::validate(db, schema, reference)?;
    if reference.uploader() != device || !crate::file_queue::queued(db, reference)? {
        return Ok(None);
    }
    let id = reference.id().to_string();
    let (_, file) = file_row::declaration(schema, reference.table())?;
    let lock = directory.lock_read_only()?;
    let (reader, original) = match file.provenance {
        Provenance::UserProvided => {
            let user = crate::user_file::read(db, schema, reference.table(), reference.key())?
                .ok_or(DbError::DamagedDatabase)?;
            (
                FileReader::open_original(&user.path, user.size, user.modified_at),
                Some(user.path),
            )
        }
        Provenance::AppProvided => {
            let identity = reference.identity()?;
            let names = db.query("SELECT path FROM _coven_device_files WHERE table_name=?1 AND key=?2 AND column_name=?3 AND identity=?4", (&reference.row.table, &reference.row.key, &reference.column, identity), |row| row.get::<_, String>(0))?;
            let name = names.into_iter().next().ok_or(DbError::DamagedDatabase)?;
            let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
            (
                directory.file(FileArea::AppProvided, &name).open_reader(),
                None,
            )
        }
    };
    let reader = reader.map_err(|error| observation(error, &id, original.is_some()))?;
    if !reference
        .matches_content(&reader)
        .map_err(|error| observation(error, &id, original.is_some()))?
    {
        return Err(changed(&id, original.as_ref()));
    }
    Ok(Some(LocalFileStream {
        file: Arc::new(LocalFileReader {
            reader,
            _lock: lock,
            id,
            original,
        }),
    }))
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
