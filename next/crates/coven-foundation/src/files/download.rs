//! Download publication retains a sibling link until the database accepts it.

use super::{
    AtomicFile, FileError, FileName, FileReader, FileWriter, ObservationError, StoreReadLock,
};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// The recorded destination of an unfinished download (§16.1, §18).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadLocation {
    /// A name reserved in the store's file area.
    AppProvided(FileName),
    /// A user-selected destination and a reserved temporary sibling name.
    UserProvided {
        /// Absolute destination, with its parent resolved before recording.
        path: PathBuf,
        /// The sibling retained until the row write commits.
        name: FileName,
    },
}

/// One operation's disk work; the journal owns these bytes until attachment.
pub struct DownloadFile {
    staged: AtomicFile,
    destination: Option<PathBuf>,
    lock: StoreReadLock,
}

impl DownloadFile {
    pub(crate) fn new(
        staged: AtomicFile,
        destination: Option<PathBuf>,
        lock: StoreReadLock,
    ) -> Self {
        Self {
            staged,
            destination,
            lock,
        }
    }

    /// Resolve the parent and reject any existing entry, including dangling links.
    /// Publication checks again atomically, since another writer can take the path.
    pub fn check_destination(path: &Path) -> Result<PathBuf, FileError> {
        let name = path.file_name().ok_or_else(|| {
            FileError::at(
                "download destination",
                path,
                io::Error::new(io::ErrorKind::InvalidInput, "file name required"),
            )
        })?;
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let parent = fs::canonicalize(parent)
            .map_err(|e| FileError::at("resolve destination parent", path, e))?;
        let path = parent.join(name);
        match fs::symlink_metadata(&path) {
            Ok(_) => Err(FileError::at(
                "check download destination",
                &path,
                io::Error::new(io::ErrorKind::AlreadyExists, "destination exists"),
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(path),
            Err(e) => Err(FileError::at("check download destination", &path, e)),
        }
    }

    /// Create the recorded staging name exclusively. Partial bytes remain owned
    /// by the journal, and a repeated unfinished download removes them first.
    pub fn create_writer(self) -> Result<FileWriter, FileError> {
        self.staged.create_writer(self.lock)
    }

    /// Install the complete file without replacing anything. The retained hard
    /// link makes a retry distinguish this publication from an unrelated file.
    /// A destination filesystem without hard links fails with its native error.
    pub fn publish(&self) -> Result<(), FileError> {
        let Some(destination) = &self.destination else {
            return Ok(());
        };
        self.staged.publish_link(destination)
    }

    /// Read the downloaded bytes, keeping the actual open file's identity.
    pub fn open_reader(&self) -> Result<FileReader, ObservationError> {
        match &self.destination {
            Some(path) => FileReader::open(path),
            None => self.staged.open_reader(),
        }
    }

    /// Remove an unaccepted copy and its staging link. An unrelated replacement
    /// at the user's path is never removed, even after a crash during publication.
    pub fn remove_unused(&self) -> Result<(), FileError> {
        if let Some(destination) = &self.destination {
            self.staged.remove_link(destination)?;
        }
        self.staged.remove()
    }

    /// After the row accepts a user file, release only the staging link. The
    /// destination is now a user original and coven never removes it.
    pub fn remove_staging(&self) -> Result<(), FileError> {
        self.staged.remove()
    }
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
