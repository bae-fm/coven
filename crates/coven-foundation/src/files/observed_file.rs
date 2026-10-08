//! Observe a user's original without acquiring any ability to modify it.

use super::FileError;
use std::{
    fs::Metadata,
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::io::AsyncReadExt;

/// Facts captured while reading one original; validation never rereads its bytes.
#[derive(Debug)]
pub struct ObservedFile {
    path: PathBuf,
    metadata: Metadata,
    modified_at: SystemTime,
}

/// A missing or changed original is distinct from an operating-system failure.
#[derive(Debug, thiserror::Error)]
pub enum ObservationError {
    /// The original no longer exists.
    #[error("file is missing: {}", .0.display())]
    Missing(PathBuf),
    /// The file changed during or after the read.
    #[error("file changed: {}", .0.display())]
    Changed(PathBuf),
    /// The filesystem refused an operation.
    #[error(transparent)]
    File(#[from] FileError),
}

impl ObservationError {
    pub(super) fn at(operation: &'static str, path: &Path, error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::NotFound {
            Self::Missing(path.to_owned())
        } else {
            FileError::at(operation, path, error).into()
        }
    }
}

impl ObservedFile {
    /// The original's path.
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Its size at the time of the read.
    pub fn size(&self) -> u64 {
        self.metadata.len()
    }
    /// Its modification time at the time of the read.
    pub fn modified_at(&self) -> SystemTime {
        self.modified_at
    }

    /// Refuse a missing, replaced or modified original without reading its bytes.
    pub fn validate(&self) -> Result<(), ObservationError> {
        let metadata = std::fs::metadata(&self.path)
            .map_err(|e| ObservationError::at("read original", &self.path, e))?;
        self.check(&metadata)
    }

    fn check(&self, metadata: &Metadata) -> Result<(), ObservationError> {
        let modified = metadata
            .modified()
            .map_err(|e| FileError::at("read modification time", &self.path, e))?;
        let mut same =
            metadata.is_file() && metadata.len() == self.size() && modified == self.modified_at;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            same &= metadata.dev() == self.metadata.dev()
                && metadata.ino() == self.metadata.ino()
                && metadata.ctime() == self.metadata.ctime()
                && metadata.ctime_nsec() == self.metadata.ctime_nsec();
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            same &= metadata.creation_time() == self.metadata.creation_time();
        }
        if same {
            Ok(())
        } else {
            Err(ObservationError::Changed(self.path.clone()))
        }
    }
}

/// Read an original once in bounded chunks, checking its handle and path before
/// returning. The consumer can hash the bytes and report progress.
pub async fn observe_file(
    path: &Path,
    mut consume: impl FnMut(&[u8]),
) -> Result<ObservedFile, ObservationError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| ObservationError::at("read original", path, e))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|e| ObservationError::at("read original", path, e))?;
    if !metadata.is_file() {
        return Err(FileError::at(
            "read original",
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "a regular file is required"),
        )
        .into());
    }
    let modified_at = metadata
        .modified()
        .map_err(|e| FileError::at("read modification time", path, e))?;
    let observed = ObservedFile {
        path: path.to_owned(),
        metadata,
        modified_at,
    };
    let mut buffer = [0; 64 * 1024];
    let mut size = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|e| ObservationError::at("read original", path, e))?;
        if read == 0 {
            break;
        }
        size += read as u64;
        consume(&buffer[..read]);
    }
    if size != observed.size() {
        return Err(ObservationError::Changed(path.to_owned()));
    }
    observed.check(
        &file
            .metadata()
            .await
            .map_err(|e| ObservationError::at("read original", path, e))?,
    )?;
    observed.validate()?;
    Ok(observed)
}
