//! Open read-only handles for positioned reads.

use super::{FileError, ObservationError};
use std::{
    fs::{File, Metadata},
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// An open read-only file. Renaming or unlinking its path never redirects reads.
/// Reads return what's on disk, as the platform's own positioned reads do.
pub struct FileReader {
    file: File,
    path: PathBuf,
    size: u64,
}

impl FileReader {
    /// Open a recorded user original and check its size and modification
    /// time, once: later reads return what's on disk.
    pub fn open_original(
        path: &Path,
        size: u64,
        modified: SystemTime,
    ) -> Result<Self, ObservationError> {
        let (file, metadata) = open_file(path)?;
        let opened = metadata
            .modified()
            .map_err(|source| FileError::at("read modification time", path, source))?;
        if metadata.len() != size || opened != modified {
            return Err(ObservationError::Changed(path.to_owned()));
        }
        Ok(Self {
            file,
            path: path.to_owned(),
            size,
        })
    }

    pub(crate) fn open(path: &Path) -> Result<Self, ObservationError> {
        let (file, metadata) = open_file(path)?;
        Ok(Self {
            file,
            path: path.to_owned(),
            size: metadata.len(),
        })
    }

    /// The size captured on this open handle.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Read exactly the requested bytes using the open file's positioned-read API.
    /// A range beyond the captured size fails instead of returning a short read,
    /// and a file found shorter than that range fails as changed.
    pub fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, ObservationError> {
        #[cfg(any(test, feature = "test-utils"))]
        crate::test_work::file_range_read();
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > self.size())
        {
            return Err(FileError::at(
                "read file range",
                &self.path,
                io::Error::new(io::ErrorKind::InvalidInput, "range exceeds file size"),
            )
            .into());
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|source| {
            FileError::at(
                "allocate file range",
                &self.path,
                io::Error::new(io::ErrorKind::OutOfMemory, source),
            )
        })?;
        bytes.resize(len, 0);
        self.read_exact_at(offset, &mut bytes)?;
        Ok(bytes)
    }

    fn read_exact_at(&self, mut offset: u64, mut bytes: &mut [u8]) -> Result<(), ObservationError> {
        while !bytes.is_empty() {
            #[cfg(unix)]
            let count = {
                use std::os::unix::fs::FileExt;
                self.file.read_at(bytes, offset)
            };
            #[cfg(windows)]
            let count = {
                use std::os::windows::fs::FileExt;
                self.file.seek_read(bytes, offset)
            };
            match count {
                Ok(0) => return Err(ObservationError::Changed(self.path.clone())),
                Ok(count) => {
                    bytes = &mut bytes[count..];
                    offset += count as u64;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(ObservationError::at("read file", &self.path, error)),
            }
        }
        Ok(())
    }
}

/// Open a regular file for positioned reads, with its metadata.
fn open_file(path: &Path) -> Result<(File, Metadata), ObservationError> {
    let file = File::open(path).map_err(|error| ObservationError::at("read file", path, error))?;
    let metadata = file
        .metadata()
        .map_err(|error| ObservationError::at("read file", path, error))?;
    if !metadata.is_file() {
        return Err(FileError::at(
            "open file for ranges",
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "a regular file is required"),
        )
        .into());
    }
    Ok((file, metadata))
}

#[cfg(test)]
#[path = "file_reader_tests.rs"]
mod tests;
