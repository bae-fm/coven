//! Download publication renames a temporary sibling without replacing a destination.

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
        /// The temporary sibling consumed by publication.
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
            Ok(_) => Err(destination_exists(&path)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(path),
            Err(e) => Err(FileError::at("check download destination", &path, e)),
        }
    }

    /// Create the recorded staging name exclusively. Partial bytes remain owned
    /// by the journal, and a repeated unfinished download removes them first.
    pub fn create_writer(self) -> Result<FileWriter, FileError> {
        self.staged.create_writer(self.lock)
    }

    /// Rename the complete temporary sibling without replacing anything.
    /// Unsupported filesystems return the native no-replace rename error.
    /// Recovery must check the destination's content before accepting a retry.
    pub fn publish(&self) -> Result<(), FileError> {
        let Some(destination) = &self.destination else {
            return Ok(());
        };
        self.staged.publish(destination)
    }

    /// Recover an unrecorded rename only when the temporary file is absent and
    /// the destination matches the recorded size and content hash. The caller
    /// supplies that content check; foundation owns the filesystem checks.
    /// A different existing destination returns AlreadyExists, never replaces.
    pub fn recover_publication(
        &self,
        matches: impl FnOnce(&FileReader) -> Result<bool, ObservationError>,
    ) -> Result<bool, ObservationError> {
        let Some(reader) = self.published_reader()? else {
            return Ok(false);
        };
        let destination = self.destination.as_ref().expect("user publication");
        if !matches(&reader)? {
            return Err(destination_exists(destination).into());
        }
        // Repeat the barrier if the crash followed rename but preceded sync.
        super::atomic_file::sync_publication(destination)?;
        Ok(true)
    }

    /// Read the downloaded bytes, keeping the actual open file's identity.
    pub fn open_reader(&self) -> Result<FileReader, ObservationError> {
        match &self.destination {
            Some(path) => FileReader::open(path),
            None => self.staged.open_reader(),
        }
    }

    /// Remove an abandoned download. A destination is owned only if its
    /// temporary sibling is absent and its recorded size and hash match.
    /// Preserve another file at that path, including a replacement after rename.
    pub fn remove_unused(
        &self,
        matches: impl FnOnce(&FileReader) -> Result<bool, ObservationError>,
    ) -> Result<(), ObservationError> {
        match self.published_reader() {
            Ok(Some(reader)) => {
                let owned = matches(&reader)?;
                drop(reader);
                let destination = self.destination.as_ref().expect("user publication");
                if owned {
                    super::atomic_file::remove(destination)?;
                } else {
                    tracing::debug!(path = %destination.display(), "preserving different destination content");
                }
            }
            Ok(None) => (),
            Err(ObservationError::File(FileError::Io { path, source, .. }))
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                tracing::debug!(path = %path.display(), "preserving another file at the destination");
            }
            Err(error) => return Err(error),
        }
        self.remove_staging()?;
        Ok(())
    }

    /// Remove partial staging bytes before restarting an unfinished download.
    pub fn remove_staging(&self) -> Result<(), FileError> {
        self.staged.remove()
    }

    fn published_reader(&self) -> Result<Option<FileReader>, ObservationError> {
        let Some(destination) = &self.destination else {
            return Ok(None);
        };
        let metadata = match fs::symlink_metadata(destination) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(
                    FileError::at("inspect download destination", destination, source).into(),
                )
            }
        };
        if self.staged.exists()? || !metadata.is_file() {
            return Err(destination_exists(destination).into());
        }
        FileReader::open(destination).map(Some)
    }
}

fn destination_exists(path: &Path) -> FileError {
    FileError::at(
        "check download destination",
        path,
        io::Error::new(io::ErrorKind::AlreadyExists, "destination exists"),
    )
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
