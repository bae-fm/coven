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
