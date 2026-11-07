//! Durable file creation, atomic replacement and removal.

#[cfg(unix)]
use super::flush_batch::BatchFolder;
use super::{FlushBatch, StoreReadLock};
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
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The new bytes are visible, but syncing the directory failed.
    #[error("sync directory after replacing {}: {source}", path.display())]
    AfterReplace {
        /// The replaced file.
        path: PathBuf,
        /// The operating system's error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The file is absent, but syncing its directory failed.
    #[error("sync directory after removing {}: {source}", path.display())]
    AfterRemove {
        /// The removed file.
        path: PathBuf,
        /// The operating system's error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Removing an unpublished temporary file also failed.
    #[error("{operation}; removing temporary file failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        #[source]
        operation: Box<FileError>,
        /// The error removing the temporary file.
        cleanup: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The device is locked, and the file's protection class keeps it from
    /// being made, opened or given its class until the device unlocks
    /// (api.md E1). The app waits for the unlock and retries.
    #[error("{}: the device is locked, and the file is protected until it unlocks", path.display())]
    DeviceLocked {
        /// The protected file, or the file that couldn't be made in a
        /// protected folder.
        path: PathBuf,
    },
    /// The operation's runtime is shutting down and dropped it unstarted
    /// (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// The operation panicked on the blocking pool: a bug, which the caller
    /// reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl crate::task::CarriesBug for FileError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Cleanup { operation, .. } => operation.bug(),
            _ => None,
        }
    }
}

impl From<crate::task::TaskFailed> for FileError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

impl FileError {
    /// Whether readers already see the replacement despite a durability error.
    pub fn installed_new_bytes(&self) -> bool {
        matches!(self, Self::AfterReplace { .. })
    }

    pub(crate) fn at(operation: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io { operation, path: path.to_owned(), source: Box::new(source) }
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
        install(prepare(&self.path, bytes, &own_temporaries(&self.path))?, &self.path)
    }

    /// Write and sync complete bytes at this name, then flush its directory
    /// and the drive's cache, as a batch of this one file, with no temporary
    /// sibling. The caller records the name as its own
    /// first, so bytes a crash interrupts stay recorded for removal. Anything
    /// but a regular file there is refused before a byte is written.
    pub fn write_recorded(&self, bytes: &[u8]) -> Result<(), FileError> {
        let path = self.resolved()?;
        #[cfg(any(test, feature = "test-utils"))]
        super::reached::reached(&path);
        let mut file = owned_options()
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|source| FileError::at("create owned file", &path, source))?;
        let regular = file
            .metadata()
            .map_err(|source| FileError::at("inspect owned file", &path, source))?
            .is_file();
        if !regular {
            return Err(FileError::at(
                "create owned file",
                &path,
                io::Error::new(io::ErrorKind::InvalidInput, "a regular file is required"),
            ));
        }
        file.write_all(bytes)
            .and_then(|()| sync_file(&file))
            .map_err(|source| FileError::at("write owned file", &path, source))?;
        #[cfg(unix)]
        sync_directory(parent(&path))
            .map_err(|source| FileError::at("sync owned file directory", &path, source))?;
        Ok(())
    }

    /// Write `bytes` over this existing file's bytes from `offset`, which
    /// they must lie within, and hand them to the drive. Readers can see
    /// some of the old bytes and some of the new while it writes, and a
    /// crash can leave them so: only bytes that check themselves, as a
    /// cache's authenticated chunks do, are written this way.
    pub fn overwrite_at(&self, offset: u64, bytes: &[u8]) -> Result<(), super::ObservationError> {
        let path = self.resolved()?;
        #[cfg(any(test, feature = "test-utils"))]
        super::reached::reached(&path);
        let file = owned_options()
            .open(&path)
            .map_err(|error| super::ObservationError::at("open owned file", &path, error))?;
        let metadata =
            file.metadata().map_err(|source| FileError::at("inspect owned file", &path, source))?;
        if !metadata.is_file()
            || offset.checked_add(bytes.len() as u64).is_none_or(|end| end > metadata.len())
        {
            return Err(FileError::at(
                "overwrite owned file",
                &path,
                io::Error::new(io::ErrorKind::InvalidInput, "a range within a regular file"),
            )
            .into());
        }
        // No directory entry changes, so `sync_all`, which flushes an Apple
        // drive's cache with `F_FULLFSYNC`, is the whole barrier.
        write_all_at(&file, offset, bytes)
            .and_then(|()| file.sync_all())
            .map_err(|source| FileError::at("overwrite owned file", &path, source))?;
        Ok(())
    }

    /// Give this existing file `length` bytes, cutting it short or adding
    /// zeros at its end, and hand the change to the drive.
    pub fn set_len(&self, length: u64) -> Result<(), super::ObservationError> {
        let path = self.resolved()?;
        #[cfg(any(test, feature = "test-utils"))]
        super::reached::reached(&path);
        let file = owned_options()
            .open(&path)
            .map_err(|error| super::ObservationError::at("open owned file", &path, error))?;
        let regular =
            file.metadata().map_err(|source| FileError::at("inspect owned file", &path, source))?;
        if !regular.is_file() {
            return Err(FileError::at(
                "resize owned file",
                &path,
                io::Error::new(io::ErrorKind::InvalidInput, "a regular file is required"),
            )
            .into());
        }
        // No directory entry changes, so `sync_all`, which flushes an Apple
        // drive's cache with `F_FULLFSYNC`, is the whole barrier.
        file.set_len(length)
            .and_then(|()| file.sync_all())
            .map_err(|source| FileError::at("resize owned file", &path, source))?;
        Ok(())
    }

    /// The path under its canonical parent, which also supplies Windows'
    /// extended-length prefix.
    fn resolved(&self) -> Result<PathBuf, FileError> {
        let directory = fs::canonicalize(parent(&self.path))
            .map_err(|source| FileError::at("resolve parent directory", &self.path, source))?;
        let name = self.path.file_name().ok_or_else(|| {
            FileError::at(
                "create owned file",
                &self.path,
                io::Error::new(io::ErrorKind::InvalidInput, "file name required"),
            )
        })?;
        Ok(directory.join(name))
    }

    /// Remove the owned file; an absent file is success. On Unix, sync the
    /// directory even on a retry after an earlier directory-sync failure.
    /// A missing parent is already absent and needs no durability barrier.
    pub fn remove(&self) -> Result<(), FileError> {
        remove(&self.path)
    }

    /// Remove the owned file and every temporary sibling a write of it left,
    /// as a crash or a failed removal does: each holds a whole version of its
    /// bytes. The caller holds what guards this file's writes, so none of
    /// them is a write in flight.
    pub fn remove_with_temporaries(&self) -> Result<(), FileError> {
        let prefix = own_temporaries(&self.path);
        remove_named(parent(&self.path), |name| {
            let random = name.strip_prefix(prefix.as_bytes());
            random.is_some_and(|random| random.len() == TEMPORARY_RANDOM)
        })?;
        remove(&self.path)
    }

    /// Leave a temporary sibling holding `bytes`, as a crash between writing
    /// it and renaming it does, and say where.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn leave_temporary(&self, bytes: &[u8]) -> PathBuf {
        let temp = prepare(&self.path, bytes, &own_temporaries(&self.path)).unwrap();
        temp.keep().expect("a temporary file can be kept").1
    }
}

/// An unpublished file's open writer. Its OS handle and path stay private.
/// Dropping it leaves the named bytes for the caller's recorded cleanup.
/// The shared store lock lasts until its handle and any cancelled I/O finish,
/// and, for a file finished into a batch, until the batch's flush ends.
pub struct FileWriter {
    file: Arc<tokio::sync::Mutex<LockedWriter>>,
    path: PathBuf,
    /// Where a panic in a write its caller stopped waiting for goes (E15).
    bugs: crate::task::BugSinkRef,
}
struct LockedWriter {
    file: fs::File,
    // Close the handle before allowing the store directory to be deleted.
    // Finishing into a batch moves it to the batch, which outlives the handle.
    lock: Option<StoreReadLock>,
}

impl FileWriter {
    /// Create `file` for asynchronous streaming without replacing anything,
    /// holding `lock`, its store's read lock, until the handle and any
    /// cancelled I/O finish, or a batch it finishes into flushes. A panic in
    /// a write whose caller stopped waiting goes to `bugs` (E15).
    pub(super) fn create(
        file: &AtomicFile,
        lock: StoreReadLock,
        bugs: crate::task::BugSinkRef,
    ) -> Result<Self, FileError> {
        let path = file.resolved()?;
        let file = create_new(&path)
            .map_err(|source| FileError::at("create owned file", &path, source))?;
        Ok(Self {
            file: Arc::new(tokio::sync::Mutex::new(LockedWriter { file, lock: Some(lock) })),
            path,
            bugs,
        })
    }

    /// Reopen `file`, an unfinished file of a writer that stopped, for
    /// appending after its first `length` bytes, which the caller recorded
    /// as synced: whatever lies past them is cut off. A file shorter than
    /// `length` is refused, since the bytes it lost are not appended again.
    pub(super) fn resume(
        file: &AtomicFile,
        lock: StoreReadLock,
        bugs: crate::task::BugSinkRef,
        length: u64,
    ) -> Result<Self, FileError> {
        use std::io::Seek;
        let path = file.resolved()?;
        let mut file = owned_options()
            .open(&path)
            .map_err(|source| FileError::at("reopen owned file", &path, source))?;
        let metadata =
            file.metadata().map_err(|source| FileError::at("inspect owned file", &path, source))?;
        if !metadata.is_file() || metadata.len() < length {
            return Err(FileError::at(
                "reopen owned file",
                &path,
                io::Error::new(io::ErrorKind::InvalidInput, "a regular file holding its bytes"),
            ));
        }
        file.set_len(length)
            .and_then(|()| file.seek(io::SeekFrom::Start(length)).map(|_| ()))
            .map_err(|source| FileError::at("reopen owned file", &path, source))?;
        Ok(Self {
            file: Arc::new(tokio::sync::Mutex::new(LockedWriter { file, lock: Some(lock) })),
            path,
            bugs,
        })
    }

    /// Read once in 64 KiB chunks, reporting each chunk after its disk write,
    /// then finish the file into `batch`.
    pub async fn write_from<R: tokio::io::AsyncRead + Unpin>(
        mut self,
        reader: &mut R,
        mut written: impl FnMut(&[u8]),
        batch: &mut FlushBatch,
    ) -> Result<(), FileError> {
        use tokio::io::AsyncReadExt;
        let mut buffer = vec![0; 64 * 1024];
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
        self.finish_into(batch).await
    }

    /// Append one bounded buffer, completing its disk write before returning.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<(), FileError> {
        let bytes = bytes.to_vec();
        self.run("append owned file", move |writer, _| writer.file.write_all(&bytes)).await
    }

    /// Sync all appended bytes, then flush their directory and the drive's
    /// cache, as a batch of this one file, before publishing metadata.
    pub async fn finish(self) -> Result<(), FileError> {
        self.run("sync owned file", |writer, path| {
            sync_file(&writer.file)?;
            #[cfg(unix)]
            sync_directory(parent(path))?;
            #[cfg(not(unix))]
            let _ = path;
            Ok(())
        })
        .await
    }

    /// Sync all appended bytes and flush their directory and the drive's
    /// cache, keeping the writer open: what was appended before is durable
    /// when this returns.
    pub async fn sync(&mut self) -> Result<(), FileError> {
        self.run("sync owned file", |writer, path| {
            sync_file(&writer.file)?;
            #[cfg(unix)]
            sync_directory(parent(path))?;
            #[cfg(not(unix))]
            let _ = path;
            Ok(())
        })
        .await
    }

    /// Sync all appended bytes and count the file in `batch`, whose flush
    /// makes it durable before the commit that records it.
    pub async fn finish_into(self, batch: &mut FlushBatch) -> Result<(), FileError> {
        #[cfg(unix)]
        {
            let folder = self
                .run("sync owned file", |writer, path| {
                    sync_file(&writer.file)?;
                    BatchFolder::open(parent(path).to_owned(), writer.lock.take())
                })
                .await?;
            batch.add(folder);
        }
        // Windows' batch flush opens no folder, so needs no lock.
        #[cfg(windows)]
        {
            self.run("sync owned file", |writer, _| {
                sync_file(&writer.file)?;
                drop(writer.lock.take());
                Ok(())
            })
            .await?;
            batch.add();
        }
        Ok(())
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: &'static str,
        run: impl FnOnce(&mut LockedWriter, &Path) -> io::Result<T> + Send + 'static,
    ) -> Result<T, FileError> {
        // Acquire before spawning so later calls wait for cancelled work too.
        // The owned guard carries the OS handle and store lock into the task.
        let mut file = self.file.clone().lock_owned().await;
        let path = self.path.clone();
        crate::task::blocking_completing(operation, self.bugs.clone(), move || {
            run(&mut file, &path).map_err(|source| FileError::at(operation, &path, source))
        })
        .await?
    }
}

/// Write all of `bytes` at `offset` with the platform's positioned writes.
fn write_all_at(file: &fs::File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let (mut offset, mut bytes) = (offset, bytes);
        while !bytes.is_empty() {
            match file.seek_write(bytes, offset) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => {
                    bytes = &bytes[count..];
                    offset += count as u64;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

/// Hand a file's bytes to the drive. Unix's `fsync` can leave them in an
/// Apple drive's cache until a flush of their directory with `F_FULLFSYNC`;
/// Windows' `FlushFileBuffers` also flushes the drive's cache.
pub(super) fn sync_file(file: &fs::File) -> io::Result<()> {
    #[cfg(unix)]
    {
        rustix::fs::fsync(file).map_err(Into::into)
    }
    #[cfg(windows)]
    {
        file.sync_all()
    }
}

/// Inspect the directory entry itself, including dangling symlinks.
/// Only a missing entry is false; all other failures retain the caller's operation.
pub(super) fn exists(operation: &'static str, path: &Path) -> Result<bool, FileError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(FileError::at(operation, path, source)),
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
            return Err(FileError::AfterRemove { path: path.to_owned(), source: Box::new(source) })
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

/// The complete file at `path`, which must exist, for a test reading back
/// bytes it saved with [`replace`].
#[cfg(any(test, feature = "test-utils"))]
pub fn read_existing(path: &Path) -> Result<Vec<u8>, FileError> {
    read_optional(path)?
        .ok_or_else(|| FileError::at("read file", path, io::ErrorKind::NotFound.into()))
}

/// The JSON record at `path`, or `None` when it is absent. Bytes that don't
/// decode are invalid data, reported as `operation`'s failure.
pub(crate) fn read_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    operation: &'static str,
) -> Result<Option<T>, FileError> {
    read_optional(path)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                FileError::at(operation, path, io::Error::new(io::ErrorKind::InvalidData, error))
            })
        })
        .transpose()
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

/// Write and sync a temporary sibling, atomically replace `path`, then sync
/// its directory. The parent must exist. Concurrent readers see whole versions.
/// A pre-replacement failure leaves the target unchanged; `FileError::AfterReplace`
/// reports that new bytes are visible but directory syncing failed.
/// Windows requires the POSIX replacement support described by [`AtomicFile::replace`].
pub fn replace(path: &Path, bytes: &[u8]) -> Result<(), FileError> {
    install(prepare(path, bytes, TEMPORARY)?, path)
}

/// Remove every temporary sibling in `folder`, the top of a store's
/// directory: what a crash or a failed removal left of its writes. The caller
/// holds the store's writer or bootstrap lock, so none is a write in flight.
pub(super) fn remove_temporaries(folder: &Path) -> Result<(), FileError> {
    remove_named(folder, |name| name.starts_with(TEMPORARY.as_bytes()))
}

/// Remove each entry of `folder` whose name `named` picks. A folder that is
/// absent holds none.
fn remove_named(folder: &Path, named: impl Fn(&[u8]) -> bool) -> Result<(), FileError> {
    let entries = match fs::read_dir(folder) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::debug!(path = %folder.display(), "folder is absent");
            return Ok(());
        }
        Err(source) => return Err(FileError::at("list folder", folder, source)),
    };
    for entry in entries {
        let entry = entry.map_err(|source| FileError::at("list folder", folder, source))?;
        if named(entry.file_name().as_encoded_bytes()) {
            remove(&entry.path())?;
        }
    }
    Ok(())
}

/// How every temporary sibling's name starts.
const TEMPORARY: &str = ".coven-write-";

/// The random letters ending a temporary sibling's name, so each writer
/// has its own.
const TEMPORARY_RANDOM: usize = 6;

/// How the names of an owned file's temporary siblings start: they carry
/// its name, so forgetting it finds them. An app's own path, written by
/// [`replace`], may already fill the 255 bytes a name holds, so it carries
/// none.
fn own_temporaries(path: &Path) -> String {
    let name = path.file_name().expect("an owned file has a name").to_string_lossy();
    format!("{TEMPORARY}{name}-")
}

/// Replace `path` with the prepared temporary sibling `temp`, whose failure
/// to install also removes it.
fn install(temp: tempfile::NamedTempFile, path: &Path) -> Result<(), FileError> {
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
            source: Box::new(source),
        })
    }
    #[cfg(windows)]
    {
        let mut temp = temp;
        if let Err(source) = windows_rename(temp.as_file(), path) {
            return Err(cleanup(temp, FileError::at("POSIX rename temporary file", path, source)));
        }
        // The source no longer exists. Disarm removal without issuing another
        // filesystem operation against a possibly reused name.
        temp.disable_cleanup(true);
        Ok(())
    }
}

fn prepare(path: &Path, bytes: &[u8], prefix: &str) -> Result<tempfile::NamedTempFile, FileError> {
    let mut temp = temporary(path, prefix)?;
    if let Err(source) = temp.write_all(bytes).and_then(|()| temp.as_file().sync_all()) {
        return Err(cleanup(temp, FileError::at("write and sync temporary file", path, source)));
    }
    Ok(temp)
}

fn temporary(path: &Path, prefix: &str) -> Result<tempfile::NamedTempFile, FileError> {
    // Canonicalization also supplies Windows' extended-length prefix. The
    // directory already exists, so there is no guess about a relative parent.
    let directory = fs::canonicalize(parent(path))
        .map_err(|source| FileError::at("resolve parent directory", path, source))?;
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix).rand_bytes(TEMPORARY_RANDOM);
    #[cfg(unix)]
    let temp = builder.tempfile_in(directory);
    #[cfg(windows)]
    let temp = builder.make_in(directory, create_new);
    temp.map_err(|source| FileError::at("create temporary file", path, source))
}

pub(super) fn create_new(path: &Path) -> io::Result<fs::File> {
    owned_options().create_new(true).open(path)
}

/// Write access to a file coven owns, durable on every platform.
fn owned_options() -> fs::OpenOptions {
    let mut options = fs::OpenOptions::new();
    options.write(true);
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
    options
}

fn cleanup(temp: tempfile::NamedTempFile, operation: FileError) -> FileError {
    match temp.close() {
        Ok(()) => operation,
        Err(cleanup) => {
            FileError::Cleanup { operation: Box::new(operation), cleanup: Box::new(cleanup) }
        }
    }
}

pub(super) fn parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Sync a directory and, on Apple systems, flush the drive's cache with
/// `F_FULLFSYNC`, as std's `sync_all` does there.
#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
#[expect(
    clippy::map_err_ignore,
    reason = "a rename buffer past u32 bytes means the path is too long"
)]
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
    let name: Vec<_> = destination.as_os_str().encode_wide().chain(Some(0)).collect();
    if name[..name.len() - 1].contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "NUL in file name"));
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
            info.cast::<u8>().add(offset_of!(FILE_RENAME_INFO, FileName)).cast::<u16>(),
            name.len(),
        );
        SetFileInformationByHandle(file.as_raw_handle(), FileRenameInfoEx, info.cast(), buffer_size)
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
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    // Both paths have a resolved parent on the same filesystem. Refuse any
    // existing destination, whether publishing a file or a store directory.
    let from = verbatim(from)?;
    let to = verbatim(to)?;
    // SAFETY: both paths are live, NUL-terminated UTF-16 buffers. They have
    // the same parent; no replacement or cross-volume copy is requested.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// `path` as a NUL-terminated `\\?\` path for a Win32 call. Given a plain
/// path, such a call parses it itself and stops at 260 characters; a `\\?\`
/// path goes to the file system as given, at any length, as std passes its
/// own calls once a path nears that limit. The path is first made absolute
/// as Win32 would read it (`GetFullPathNameW`), which refuses a NUL; then
/// `C:\…` becomes `\\?\C:\…` and `\\server\share\…` becomes
/// `\\?\UNC\server\share\…`. A `\\?\` or `\\.\` path stays as it is.
#[cfg(windows)]
fn verbatim(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    use std::path::{Component, Prefix};

    let absolute = std::path::absolute(path)?;
    let Some(Component::Prefix(prefix)) = absolute.components().next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an absolute path without a drive or share",
        ));
    };
    let units = absolute.as_os_str().encode_wide();
    let units: Vec<u16> = match prefix.kind() {
        Prefix::Disk(_) => r"\\?\".encode_utf16().chain(units).collect(),
        // `\\server` gives one of its backslashes to `\\?\UNC\`.
        Prefix::UNC(..) => r"\\?\UNC".encode_utf16().chain(units.skip(1)).collect(),
        Prefix::Verbatim(_)
        | Prefix::VerbatimUNC(..)
        | Prefix::VerbatimDisk(_)
        | Prefix::DeviceNS(_) => units.collect(),
    };
    Ok(units.into_iter().chain(Some(0)).collect())
}

pub(super) fn sync_publication(path: &Path) -> Result<(), FileError> {
    #[cfg(unix)]
    sync_directory(parent(path)).map_err(|source| FileError::AfterReplace {
        path: path.to_owned(),
        source: Box::new(source),
    })?;
    // MoveFileExW uses WRITE_THROUGH; Unix needs the explicit parent barrier.
    #[cfg(windows)]
    let _ = path;
    Ok(())
}

#[cfg(test)]
#[path = "atomic_file_tests.rs"]
mod tests;
