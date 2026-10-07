//! Open read-only handles with positioned reads and stable file facts.

use super::{FileError, ObservationError};
use std::{
    fs::{File, Metadata},
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// An open read-only file. Renaming or unlinking its path never redirects reads.
/// Callers validate a content hash by scanning, then retain this handle for ranges.
pub struct FileReader {
    file: File,
    path: PathBuf,
    metadata: Metadata,
    modified: SystemTime,
}

impl FileReader {
    /// Open a recorded user original and check its size and modification time.
    pub fn open_original(
        path: &Path,
        size: u64,
        modified: SystemTime,
    ) -> Result<Self, ObservationError> {
        let reader = Self::open(path)?;
        if reader.metadata.len() != size || reader.modified != modified {
            return Err(ObservationError::Changed(path.to_owned()));
        }
        Ok(reader)
    }

    pub(crate) fn open(path: &Path) -> Result<Self, ObservationError> {
        let file = File::open(path).map_err(|error| observed_error(path, error))?;
        let metadata = file
            .metadata()
            .map_err(|error| observed_error(path, error))?;
        if !metadata.is_file() {
            return Err(FileError::at(
                "open file for ranges",
                path,
                io::Error::new(io::ErrorKind::InvalidInput, "a regular file is required"),
            )
            .into());
        }
        let modified = metadata
            .modified()
            .map_err(|source| FileError::at("read modification time", path, source))?;
        Ok(Self {
            file,
            path: path.to_owned(),
            metadata,
            modified,
        })
    }

    /// The size captured on this open handle.
    pub fn size(&self) -> u64 {
        self.metadata.len()
    }

    /// Read the whole open file in bounded chunks to check its content.
    pub fn scan(&self, mut consume: impl FnMut(&[u8])) -> Result<(), ObservationError> {
        let mut offset = 0;
        let mut buffer = [0; 64 * 1024];
        self.validate()?;
        while offset < self.size() {
            let length = (self.size() - offset).min(buffer.len() as u64) as usize;
            self.read_exact_at(offset, &mut buffer[..length])?;
            consume(&buffer[..length]);
            offset += length as u64;
        }
        self.validate()
    }

    /// Read exactly the requested bytes using the open file's positioned-read API.
    /// A range beyond the captured size fails instead of returning a short read.
    pub fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, ObservationError> {
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
        self.validate()?;
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
        self.validate()?;
        Ok(bytes)
    }

    fn validate(&self) -> Result<(), ObservationError> {
        let metadata = self
            .file
            .metadata()
            .map_err(|error| observed_error(&self.path, error))?;
        let modified = metadata
            .modified()
            .map_err(|source| FileError::at("read modification time", &self.path, source))?;
        if metadata.len() != self.size() || modified != self.modified {
            return Err(ObservationError::Changed(self.path.clone()));
        }
        Ok(())
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
                Err(error) => return Err(observed_error(&self.path, error)),
            }
        }
        Ok(())
    }
}

fn observed_error(path: &Path, error: io::Error) -> ObservationError {
    if error.kind() == io::ErrorKind::NotFound {
        ObservationError::Missing(path.to_owned())
    } else {
        FileError::at("read file", path, error).into()
    }
}

#[cfg(test)]
#[path = "file_reader_tests.rs"]
mod tests;
