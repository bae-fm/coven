//! Durable file creation, atomic replacement and removal.

use super::StoreReadLock;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    /// The file is absent, but syncing its directory failed.
    #[error("sync directory after removing {}: {source}", path.display())]
    AfterRemove {
        /// The removed file.
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

/// One named file capability. It reads, creates, replaces and removes bytes
/// without exposing a path. Replacement atomically installs a complete version.
#[derive(Clone, Debug)]
pub struct AtomicFile {
    path: PathBuf,
}

impl AtomicFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(super) fn publish(&self, destination: &Path) -> Result<(), FileError> {
        rename_new(&self.path, destination)
            .map_err(|source| FileError::at("publish download", destination, source))?;
        sync_publication(destination)
    }

    pub(super) fn exists(&self) -> Result<bool, FileError> {
        match fs::symlink_metadata(&self.path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(FileError::at("inspect staged file", &self.path, source)),
        }
    }

    /// Read the complete file. Only a missing file is `None`; other errors
    /// retain their operating-system cause.
    pub fn read_optional(&self) -> Result<Option<Vec<u8>>, FileError> {
        read_optional(&self.path)
    }

    /// Open this owned file for checked scans and positioned reads.
    pub fn open_reader(&self) -> Result<super::FileReader, super::ObservationError> {
        super::FileReader::open(&self.path)
    }

    /// Write and sync a temporary sibling, replace the target, then sync its
    /// directory. The parent must already exist. Concurrent writers each
    /// install one complete version; a failed pre-rename write leaves it alone.
    ///
    /// Windows uses POSIX replacement on a write-through file handle, which
    /// flushes the rename's metadata on NTFS. This requires `FileRenameInfoEx`
    /// and a filesystem supporting POSIX replacement. Windows 10 before 1607
    /// lacks that API; unsupported systems return the OS error without falling
    /// back to a rename that can fail while readers hold old versions open.
    pub fn replace(&self, bytes: &[u8]) -> Result<(), FileError> {
        replace(&self.path, bytes)
    }

    /// Create a named file for asynchronous streaming without replacing anything.
    /// The caller records its name first and removes partial bytes after failure.
    /// Supply this file's store read lock; every outstanding write retains it.
    pub fn create_writer(&self, lock: StoreReadLock) -> Result<FileWriter, FileError> {
        let directory = fs::canonicalize(parent(&self.path))
            .map_err(|source| FileError::at("resolve parent directory", &self.path, source))?;
        let name = self.path.file_name().ok_or_else(|| {
            FileError::at(
                "create owned file",
                &self.path,
                io::Error::new(io::ErrorKind::InvalidInput, "file name required"),
            )
        })?;
        let path = directory.join(name);
        let file = create_new(&path)
            .map_err(|source| FileError::at("create owned file", &path, source))?;
        Ok(FileWriter {
            file: Arc::new(tokio::sync::Mutex::new(LockedWriter { file, _lock: lock })),
            path,
        })
    }

    /// Remove the owned file; an absent file is success. On Unix, sync the
    /// directory even on a retry after an earlier directory-sync failure.
    /// A missing parent is already absent and needs no durability barrier.
    pub fn remove(&self) -> Result<(), FileError> {
        remove(&self.path)
    }
}

/// An unpublished file's open writer. Its OS handle and path stay private.
/// Dropping it leaves the named bytes for the caller's recorded cleanup.
/// The shared store lock lasts until its handle and any cancelled I/O finish.
pub struct FileWriter {
    file: Arc<tokio::sync::Mutex<LockedWriter>>,
    path: PathBuf,
}
struct LockedWriter {
    file: fs::File,
    // Close the handle before allowing the store directory to be deleted.
    _lock: StoreReadLock,
}

impl FileWriter {
    /// Read once in 64 KiB chunks, reporting each chunk after its disk write.
    /// Sync the bytes and their directory before returning success.
    pub async fn write_from<R: tokio::io::AsyncRead + Unpin>(
        mut self,
        reader: &mut R,
        mut written: impl FnMut(&[u8]),
    ) -> Result<(), FileError> {
        use tokio::io::AsyncReadExt;
        let mut buffer = [0; 64 * 1024];
        loop {
            let count = reader
                .read(&mut buffer)
                .await
                .map_err(|source| FileError::at("read file source", &self.path, source))?;
            if count == 0 {
                break;
            }
            self.append(&buffer[..count]).await?;
            written(&buffer[..count]);
        }
        self.finish().await
    }

    /// Append one bounded buffer, completing its disk write before returning.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<(), FileError> {
        let bytes = bytes.to_vec();
        self.run("append owned file", move |file, _| file.write_all(&bytes))
            .await
    }

    /// Sync all appended bytes and their directory before publishing metadata.
    pub async fn finish(self) -> Result<(), FileError> {
        self.run("sync owned file", |file, path| {
            file.sync_all()?;
            #[cfg(unix)]
            sync_directory(parent(path))?;
            #[cfg(not(unix))]
            let _ = path;
            Ok(())
        })
        .await
    }

    async fn run(
        &self,
        operation: &'static str,
        run: impl FnOnce(&mut fs::File, &Path) -> io::Result<()> + Send + 'static,
    ) -> Result<(), FileError> {
        // Acquire before spawning so later calls wait for cancelled work too.
        // The owned guard carries the OS handle and store lock into the task.
        let mut file = self.file.clone().lock_owned().await;
        let path = self.path.clone();
        match tokio::task::spawn_blocking(move || {
            run(&mut file.file, &path).map_err(|source| FileError::at(operation, &path, source))
        })
        .await
        {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("owned file task was cancelled: {error}"),
        }
    }
}

pub(crate) fn remove(path: &Path) -> Result<(), FileError> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "file is already absent");
        }
        Err(source) => return Err(FileError::at("remove file", path, source)),
    }
    #[cfg(unix)]
    match sync_directory(parent(path)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "file's parent is absent");
        }
        Err(source) => {
            return Err(FileError::AfterRemove {
                path: path.to_owned(),
                source,
            })
        }
    }
    Ok(())
}

pub(crate) fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, FileError> {
    let result = open_reader(path).and_then(|mut file| {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    });
    match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "file is absent");
            Ok(None)
        }
        Err(source) => Err(FileError::at("read file", path, source)),
    }
}

fn open_reader(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
    }
    options.open(path)
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
        let mut temp = temp;
        if let Err(source) = windows_rename(temp.as_file(), path) {
            return Err(cleanup(
                temp,
                FileError::at("POSIX rename temporary file", path, source),
            ));
        }
        // The source no longer exists. Disarm removal without issuing another
        // filesystem operation against a possibly reused name.
        temp.disable_cleanup(true);
        Ok(())
    }
}

fn prepare(path: &Path, bytes: &[u8]) -> Result<tempfile::NamedTempFile, FileError> {
    let mut temp = temporary(path, ".coven-write-")?;
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

fn temporary(path: &Path, prefix: &str) -> Result<tempfile::NamedTempFile, FileError> {
    // Canonicalization also supplies Windows' extended-length prefix. The
    // directory already exists, so there is no guess about a relative parent.
    let directory = fs::canonicalize(parent(path))
        .map_err(|source| FileError::at("resolve parent directory", path, source))?;
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    #[cfg(unix)]
    let temp = builder.tempfile_in(directory);
    #[cfg(windows)]
    let temp = builder.make_in(directory, create_new);
    temp.map_err(|source| FileError::at("create temporary file", path, source))
}

fn create_new(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            DELETE, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_WRITE_THROUGH, FILE_GENERIC_READ,
            FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        // Write-through also flushes creation and rename metadata on NTFS.
        // Do not mark bytes destined for durable storage as TEMPORARY.
        options
            .access_mode(DELETE | FILE_GENERIC_READ | FILE_GENERIC_WRITE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .attributes(FILE_ATTRIBUTE_NORMAL)
            .custom_flags(FILE_FLAG_WRITE_THROUGH);
    }
    options.open(path)
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
fn windows_rename(file: &fs::File, to: &Path) -> io::Result<()> {
    use std::mem::{offset_of, size_of, size_of_val, MaybeUninit};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileRenameInfoEx, SetFileInformationByHandle, FILE_RENAME_INFO,
    };
    use windows_sys::Win32::System::WindowsProgramming::{
        FILE_RENAME_FLAG_POSIX_SEMANTICS, FILE_RENAME_FLAG_REPLACE_IF_EXISTS,
    };

    let name = to
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file name required"))?;
    let destination = fs::canonicalize(parent(to))?.join(name);
    let name: Vec<_> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    if name[..name.len() - 1].contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in file name",
        ));
    }
    let too_long = || io::Error::new(io::ErrorKind::InvalidInput, "rename path is too long");
    let buffer_size = size_of::<FILE_RENAME_INFO>()
        .checked_add(size_of_val(name.as_slice()))
        .ok_or_else(too_long)?;
    let buffer_size = u32::try_from(buffer_size).map_err(|_| too_long())?;
    // Allocate in units of the ABI type for its alignment, leaving enough room
    // for its variable-length UTF-16 name, NUL and trailing struct padding.
    let mut buffer = vec![
        MaybeUninit::<FILE_RENAME_INFO>::zeroed();
        (buffer_size as usize).div_ceil(size_of::<FILE_RENAME_INFO>())
    ];
    let info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    // SAFETY: the allocation is aligned for FILE_RENAME_INFO and contains its
    // header and the whole name. RootDirectory is null (the name is absolute),
    // and FileNameLength excludes the NUL.
    // Both the buffer and the DELETE-access, write-through handle stay alive
    // for the synchronous call. No Rust reference covers the variable tail.
    let result = unsafe {
        (*info).Anonymous.Flags =
            FILE_RENAME_FLAG_POSIX_SEMANTICS | FILE_RENAME_FLAG_REPLACE_IF_EXISTS;
        (*info).FileNameLength = (size_of_val(name.as_slice()) - size_of::<u16>()) as u32;
        std::ptr::copy_nonoverlapping(
            name.as_ptr(),
            info.cast::<u8>()
                .add(offset_of!(FILE_RENAME_INFO, FileName))
                .cast::<u16>(),
            name.len(),
        );
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FileRenameInfoEx,
            info.cast(),
            buffer_size,
        )
    };
    if result == 0 {
        // No classic-rename fallback: it cannot preserve replacement while
        // old versions are open. Keep the OS cause on unsupported systems too.
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Rename a file or directory without replacing any existing destination.
/// Callers use resolved siblings and handle directory durability themselves.
/// Unsupported filesystems return their native error; there is no fallback.
#[cfg(unix)]
pub(crate) fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags, CWD};
    renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE).map_err(Into::into)
}

#[cfg(windows)]
pub(crate) fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    // Both paths have a resolved parent on the same filesystem. Refuse any
    // existing destination, whether publishing a file or a store directory.
    let from: Vec<_> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<_> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    if from[..from.len() - 1].contains(&0) || to[..to.len() - 1].contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in rename path",
        ));
    }
    // SAFETY: both paths are live, NUL-terminated UTF-16 buffers. They have
    // the same parent; no replacement or cross-volume copy is requested.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn sync_publication(path: &Path) -> Result<(), FileError> {
    #[cfg(unix)]
    sync_directory(parent(path)).map_err(|source| FileError::AfterReplace {
        path: path.to_owned(),
        source,
    })?;
    // MoveFileExW uses WRITE_THROUGH; Unix needs the explicit parent barrier.
    #[cfg(windows)]
    let _ = path;
    Ok(())
}

#[cfg(test)]
#[path = "atomic_file_tests.rs"]
mod tests;
