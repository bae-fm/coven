//! Preparing an original does no database work and never changes the file.

use crate::file_hashes::{FileHasher, FileHashes};
use crate::DbError;
use coven_foundation::files::{observe_file, observe_metadata, ObservationError, ObservedFile};
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

/// An original read and checked before entering a write.
#[derive(Debug)]
pub struct PreparedUserFile {
    pub(crate) observed: ObservedFile,
    pub(crate) hashes: FileHashes,
}

/// An original observed without reading its bytes: its path, size and
/// modification time, from a regular file.
#[derive(Debug)]
pub struct ObservedUserFile {
    pub(crate) observed: ObservedFile,
}

/// A user's original to stage, and where its file is to be: on this device
/// only, observed, or for the cloud, read whole once for its upload (§17.3).
#[derive(Debug)]
pub enum UserSource {
    /// On this device only; nothing reads its bytes until a read does.
    DeviceOnly(ObservedUserFile),
    /// Uploaded to the cloud, its bytes checked against what was read.
    Cloud(PreparedUserFile),
    /// Uploaded, as `Cloud`, and kept whole here too once stored: the write
    /// switching it to the cloud pins it on this device, which downloads it
    /// from storage. The original is never read for the cache (§17.7).
    CloudKeptHere(PreparedUserFile),
}

/// Observe a user's original without reading it: coven records its path,
/// size and modification time and computes no hash.
pub async fn observe_user_file(path: &Path) -> Result<ObservedUserFile, DbError> {
    Ok(ObservedUserFile {
        observed: observe_metadata(path).await?,
    })
}

/// The facts recorded for the user's original, which coven never changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserFile {
    /// The user's original path.
    pub path: PathBuf,
    /// Its observed size in bytes.
    pub size: u64,
    /// Its observed modification time.
    pub modified_at: SystemTime,
}

/// Read a user's original once, recording its whole-file hash and each upload
/// chunk's hash from the same bytes, and reporting total bytes read. Changes
/// during the read are refused; later ones fail its upload.
pub async fn prepare_user_file(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError> {
    let mut hash = FileHasher::new();
    let mut read = 0;
    let observed = observe_file(path, |bytes| {
        hash.update(bytes);
        read += bytes.len() as u64;
        progress(read);
    })
    .await?;
    Ok(PreparedUserFile {
        observed,
        hashes: hash.finish(),
    })
}

impl From<ObservationError> for DbError {
    fn from(error: ObservationError) -> Self {
        match error {
            ObservationError::Missing(path) => Self::UserFileMissing { path },
            ObservationError::Changed(path) => Self::UserFileChanged { path },
            ObservationError::File(error) => Self::Disk(error),
        }
    }
}

pub(crate) fn encode_time(time: SystemTime) -> Vec<u8> {
    let (sign, elapsed) = match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(elapsed) => (0, elapsed),
        Err(error) => (1, error.duration()),
    };
    let mut bytes = vec![sign];
    bytes.extend(elapsed.as_secs().to_be_bytes());
    bytes.extend(elapsed.subsec_nanos().to_be_bytes());
    bytes
}

/// The user's original this device recorded for `file`, if it has one.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn read(
    db: &crate::sqlite::DatabaseConnection,
    file: coven_foundation::id_source::FileId,
) -> Result<Option<UserFile>, DbError> {
    db.query(
        "SELECT path,size,modified_at FROM _coven_user_files WHERE file=?1",
        [file],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        },
    )?
    .into_iter()
    .next()
    .map(|(path, size, modified_at)| {
        Ok(UserFile {
            path: decode_path(path)?,
            size: u64::try_from(size).map_err(|_| DbError::DamagedDatabase)?,
            modified_at: decode_time(&modified_at)?,
        })
    })
    .transpose()
}

pub(crate) fn decode_time(bytes: &[u8]) -> Result<SystemTime, DbError> {
    if bytes.len() != 13 {
        return Err(DbError::DamagedDatabase);
    }
    let seconds = u64::from_be_bytes(bytes[1..9].try_into().expect("checked time width"));
    let nanos = u32::from_be_bytes(bytes[9..].try_into().expect("checked time width"));
    if nanos >= 1_000_000_000 {
        return Err(DbError::DamagedDatabase);
    }
    let elapsed = std::time::Duration::new(seconds, nanos);
    match bytes[0] {
        0 => std::time::UNIX_EPOCH.checked_add(elapsed),
        1 => std::time::UNIX_EPOCH.checked_sub(elapsed),
        _ => None,
    }
    .ok_or(DbError::DamagedDatabase)
}

/// A path `path_bytes` recorded; bytes it never gives are a damaged
/// database.
pub(crate) fn decode_path(bytes: Vec<u8>) -> Result<PathBuf, DbError> {
    coven_foundation::files::path_from_bytes(bytes).ok_or(DbError::DamagedDatabase)
}
