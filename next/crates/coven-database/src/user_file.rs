//! Preparing an original does no database work and never changes the file.

use crate::DbError;
use coven_crypto::{ContentHash, ContentHasher};
use coven_foundation::files::{observe_file, ObservationError, ObservedFile};
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

/// An original read and checked before entering a write.
#[derive(Debug)]
pub struct PreparedUserFile {
    pub(crate) observed: ObservedFile,
    pub(crate) hash: ContentHash,
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

/// Read a user's original once, hashing in bounded chunks and reporting total
/// bytes read. Changes during the read or before attachment are refused.
pub async fn prepare_user_file(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError> {
    let mut hash = ContentHasher::new();
    let mut read = 0;
    let observed = observe_file(path, |bytes| {
        hash.update(bytes);
        read += bytes.len() as u64;
        progress(read);
    })
    .await?;
    Ok(PreparedUserFile {
        observed,
        hash: hash.finish(),
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

pub(crate) fn encode_path(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
}

pub(crate) fn read(
    db: &crate::sqlite::DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    table: &str,
    key: &crate::RowKey,
) -> Result<Option<UserFile>, DbError> {
    let (_, file) = crate::file_row::declaration(schema, table)?;
    if file.provenance != crate::Provenance::UserProvided {
        return Ok(None);
    }
    let (key, values) = match crate::file_row::lookup(db, schema, table, key) {
        Ok(row) => row,
        Err(DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let records = db.query("SELECT path,size,modified_at FROM coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3 AND identity=?4", (&key.0,&key.1,&file.id,crate::file_row::identity(file,&values)?), |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?)))?;
    records
        .into_iter()
        .next()
        .map(|(path, size, modified_at)| {
            Ok(UserFile {
                path: decode_path(path)?,
                size: u64::from_be_bytes(size.try_into().map_err(|_| DbError::DamagedDatabase)?),
                modified_at: decode_time(&modified_at)?,
            })
        })
        .transpose()
}

fn decode_time(bytes: &[u8]) -> Result<SystemTime, DbError> {
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

fn decode_path(bytes: Vec<u8>) -> Result<PathBuf, DbError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(std::ffi::OsString::from_vec(bytes).into())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        if bytes.len() % 2 != 0 {
            return Err(DbError::DamagedDatabase);
        }
        let units: Vec<_> = bytes
            .chunks_exact(2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
            .collect();
        Ok(std::ffi::OsString::from_wide(&units).into())
    }
}
