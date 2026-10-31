//! Resolve a file still on this device only to its one local byte source.

use super::FileRef;
use crate::{sqlite::DatabaseConnection, DbError};
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
        /// The file's id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// The recorded original's facts changed.
    #[error("original {id} changed at {}", path.display())]
    UserFileChanged {
        /// The file's id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// Bytes did not match the file's content or an owned copy disappeared.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The file's id.
        id: String,
    },
    /// The requested range exceeds the file's size.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The file's id.
        id: String,
        /// The requested first byte.
        offset: u64,
        /// The requested exclusive end, saturated if addition overflows.
        end: u64,
        /// The file's whole size.
        size: u64,
    },
    /// Looking up the file failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The filesystem failed.
    #[error(transparent)]
    Disk(#[from] FileError),
    /// The store cannot be retained while opening its file.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
}

/// An open local file checked against its recorded facts. Retaining it keeps
/// the opened bytes readable across its upload completing and file
/// unlinking, and prevents store deletion until its reads and handles have
/// finished.
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

/// Open the bytes of a file on this device only, as its record here keeps
/// them: the user's original, or coven's copy. Any other file, including one
/// this device has finished uploading, returns `None`: its bytes are read
/// from the cache or storage (§17.1).
pub(crate) fn open(
    db: &DatabaseConnection,
    directory: &StoreDir,
    device: DeviceId,
    reference: &FileRef,
) -> Result<Option<LocalFileStream>, LocalFileError> {
    if reference.place() != (crate::FilePlace::DeviceOnly { device }) {
        return Ok(None);
    }
    file_source(db, reference.id())?
        .map(|source| {
            open_source(
                directory,
                &reference.id().to_string(),
                reference.size(),
                source,
            )
        })
        .transpose()
}

/// Where a file's bytes are on this device: the user's original, as
/// recorded, or coven's own copy.
pub(crate) enum LocalSource {
    Original {
        path: PathBuf,
        modified_at: std::time::SystemTime,
    },
    Copy(FileName),
}

/// The bytes this device keeps for `file`, while it is on this device only;
/// `None` for any other file. Reads only the database.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn file_source(
    db: &DatabaseConnection,
    file: coven_foundation::id_source::FileId,
) -> Result<Option<LocalSource>, DbError> {
    if let Some(original) = crate::user_file::read(db, file)? {
        return Ok(Some(LocalSource::Original {
            path: original.path,
            modified_at: original.modified_at,
        }));
    }
    db.query(
        "SELECT path FROM _coven_device_files WHERE file=?1",
        [file],
        |row| row.get::<_, String>(0),
    )?
    .into_iter()
    .next()
    .map(|name| {
        Ok(LocalSource::Copy(
            FileName::new(name).map_err(|_| DbError::DamagedDatabase)?,
        ))
    })
    .transpose()
}

/// The source an upload's entry fixed at staging; `None` once it is no
/// longer queued. Reads only the database.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn queued_source(
    db: &DatabaseConnection,
    upload: i64,
) -> Result<Option<LocalSource>, DbError> {
    let sources = db.query(
        "SELECT original,modified_at,copy FROM _coven_file_uploads WHERE id=?1",
        [upload],
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
        (Some(path), Some(modified_at), None) => Ok(Some(LocalSource::Original {
            path: crate::user_file::decode_path(path)?,
            modified_at: crate::user_file::decode_time(&modified_at)?,
        })),
        (None, None, Some(name)) => Ok(Some(LocalSource::Copy(
            FileName::new(name).map_err(|_| DbError::DamagedDatabase)?,
        ))),
        _ => Err(DbError::DamagedDatabase),
    }
}

/// Open a file's local source: it must still have the size, and an
/// original the modification time, recorded at staging. Reads then return
/// what's on disk; an upload checks each chunk it sends (§17.9). Touches
/// only the disk, never the database. `id` names the file in its errors.
pub(crate) fn open_source(
    directory: &StoreDir,
    id: &str,
    size: u64,
    source: LocalSource,
) -> Result<LocalFileStream, LocalFileError> {
    let id = id.to_owned();
    let lock = directory.lock_read_only()?;
    let (reader, original) = match source {
        LocalSource::Original { path, modified_at } => (
            FileReader::open_original(&path, size, modified_at)
                .map_err(|error| observation(error, &id, true))?,
            Some(path),
        ),
        LocalSource::Copy(name) => {
            let reader = directory
                .file(FileArea::AppProvided, &name)
                .open_reader()
                .map_err(|error| observation(error, &id, false))?;
            // The stream's size is its open handle's, which must be the file's.
            if reader.size() != size {
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
