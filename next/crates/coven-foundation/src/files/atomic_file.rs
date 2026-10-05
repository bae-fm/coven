//! Install complete bytes using a synced temporary sibling and an atomic rename.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A file operation's cause, including whether replacement already happened.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// An operation failed without replacing the target.
    #[error("{operation} {}: {source}", path.display())]
    Io {
        /// The operation that failed.
        operation: &'static str,
        /// The affected path.
        path: PathBuf,
        /// The operating system's error.
        #[source]
        source: io::Error,
    },
    /// The new bytes are visible, but syncing the directory failed.
    #[error("sync directory after replacing {}: {source}", path.display())]
    AfterReplace {
        /// The replaced file.
        path: PathBuf,
        /// The operating system's error.
        #[source]
        source: io::Error,
    },
    /// Removing an unpublished temporary file also failed.
    #[error("{operation}; removing temporary file failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        #[source]
        operation: Box<FileError>,
        /// The error removing the temporary file.
        cleanup: io::Error,
    },
}

impl FileError {
    /// Whether readers already see the replacement despite a durability error.
    pub fn installed_new_bytes(&self) -> bool {
        matches!(self, Self::AfterReplace { .. })
    }

    pub(crate) fn at(operation: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_owned(),
            source,
        }
    }
}

/// One named file capability. It reads bytes and replaces them atomically,
/// without exposing a path for callers to perform filesystem operations.
#[derive(Clone, Debug)]
pub struct AtomicFile {
    path: PathBuf,
}

impl AtomicFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Read the complete file. Only a missing file is `None`; other errors
    /// retain their operating-system cause.
    pub fn read_optional(&self) -> Result<Option<Vec<u8>>, FileError> {
        read_optional(&self.path)
    }

    /// Write and sync a temporary sibling, replace the target, then sync its
    /// directory. The parent must already exist. Concurrent writers each
    /// install one complete version; a failed pre-rename write leaves it alone.
    ///
    /// Windows uses a write-through rename for the directory-entry durability
    /// barrier; Win32 does not offer POSIX directory fsync.
    pub fn replace(&self, bytes: &[u8]) -> Result<(), FileError> {
        replace(&self.path, bytes)
    }
}

pub(crate) fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, FileError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "file is absent");
            Ok(None)
        }
        Err(source) => Err(FileError::at("read file", path, source)),
    }
}

pub(crate) fn replace(path: &Path, bytes: &[u8]) -> Result<(), FileError> {
    let temp = prepare(path, bytes)?;
    #[cfg(unix)]
    {
        match temp.persist(path) {
            Ok(file) => drop(file),
            Err(error) => {
                return Err(cleanup(
                    error.file,
                    FileError::at("rename temporary file", path, error.error),
                ));
            }
        }
        sync_directory(parent(path)).map_err(|source| FileError::AfterReplace {
            path: path.to_owned(),
            source,
        })
    }
    #[cfg(windows)]
    {
        // Close the temporary handle before renaming. The path still owns its
        // removal on failure; an explicit close reports a cleanup error too.
        let mut temp = temp.into_temp_path();
        let result = windows_rename(&temp, path, true);
        match result {
            Ok(()) => {
                // The source no longer exists. Disarm removal without issuing
                // another filesystem operation against a possibly reused name.
                temp.disable_cleanup(true);
                Ok(())
            }
            Err(source) => {
                let operation = FileError::at("rename temporary file", path, source);
                match temp.close() {
                    Ok(()) => Err(operation),
                    Err(cleanup) => Err(FileError::Cleanup {
                        operation: Box::new(operation),
                        cleanup,
                    }),
                }
            }
        }
    }
}

fn prepare(path: &Path, bytes: &[u8]) -> Result<tempfile::NamedTempFile, FileError> {
    // Canonicalization also supplies Windows' extended-length prefix. The
    // directory already exists, so there is no guess about a relative parent.
    let directory = fs::canonicalize(parent(path))
        .map_err(|source| FileError::at("resolve parent directory", path, source))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".coven-write-")
        .tempfile_in(directory)
        .map_err(|source| FileError::at("create temporary file", path, source))?;
    if let Err(source) = temp
        .write_all(bytes)
        .and_then(|()| temp.as_file().sync_all())
    {
        return Err(cleanup(
            temp,
            FileError::at("write and sync temporary file", path, source),
        ));
    }
    Ok(temp)
}

fn cleanup(temp: tempfile::NamedTempFile, operation: FileError) -> FileError {
    match temp.close() {
        Ok(()) => operation,
        Err(cleanup) => FileError::Cleanup {
            operation: Box::new(operation),
            cleanup,
        },
    }
}

fn parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
pub(crate) fn windows_rename(from: &Path, to: &Path, replace: bool) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, SetFileAttributesW, FILE_ATTRIBUTE_NORMAL, MOVEFILE_REPLACE_EXISTING,
        MOVEFILE_WRITE_THROUGH,
    };

    let name = to
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file name required"))?;
    let destination = fs::canonicalize(parent(to))?.join(name);
    let from: Vec<_> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<_> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // SAFETY: the source is a live, NUL-terminated UTF-16 buffer. A persisted
    // file must no longer have tempfile's FILE_ATTRIBUTE_TEMPORARY hint.
    if replace && unsafe { SetFileAttributesW(from.as_ptr(), FILE_ATTRIBUTE_NORMAL) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both paths are live, NUL-terminated UTF-16 buffers. The temp and
    // destination have the same parent, so this cannot become a copy/delete.
    let flags = MOVEFILE_WRITE_THROUGH
        | if replace {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "atomic_file_tests.rs"]
mod tests;
