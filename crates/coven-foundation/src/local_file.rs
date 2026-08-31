//! Private local-file machinery used by storage and store-directory capabilities.

use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncReadExt;

use crate::atomic_file::{FileError, FileSync};

/// The filename prefix an atomic blob write gives its in-progress temp sibling
/// (`.tmp.<uuid>`) before the owning durability policy and rename make it the
/// committed destination.
pub const TEMP_BLOB_PREFIX: &str = crate::atomic_file::TEMP_FILE_PREFIX;

/// Whether `path`'s file name marks it as an atomic-write temp sibling.
pub(crate) fn is_temp_blob_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(TEMP_BLOB_PREFIX))
}

#[derive(Debug)]
pub enum StreamWriteError<E> {
    Source(E),
    Local(FileError),
}

impl<E: std::fmt::Display> std::fmt::Display for StreamWriteError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => error.fmt(f),
            Self::Local(error) => write!(f, "local destination: {error}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for StreamWriteError<E> {}

#[derive(Debug)]
pub enum ByteStreamWriteError<E> {
    Source(E),
    SourceCleanup { source: E, cleanup: FileError },
    Local(FileError),
}

#[async_trait]
pub trait PlaintextChunkReader: Send {
    type Error: Send;
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, Self::Error>;
}

enum AtomicChunkWriteError<E> {
    Source(E),
    Local(FileError),
}

struct AtomicTempFile {
    path: PathBuf,
    file: Option<tokio::fs::File>,
    armed: bool,
}

/// A provider download path that becomes visible at its destination only after
/// the caller has verified the completed file.
pub struct AtomicStagedFile {
    destination: PathBuf,
    staged: Option<AtomicTempFile>,
    file_sync: FileSync,
}

/// A staged file that has been installed at its destination while the caller's
/// durable transaction is still deciding whether that installation commits.
pub struct PublishedAtomicFile {
    destination: PathBuf,
    file_sync: FileSync,
}

#[derive(Debug, thiserror::Error)]
pub enum CommitNewFileError {
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("commit new file: {0}")]
    Filesystem(#[from] FileError),
    #[error("{operation}; rollback failed: {rollback}")]
    RollbackFailed {
        operation: Box<CommitNewFileError>,
        rollback: Box<FileError>,
    },
}

impl CommitNewFileError {
    fn rollback(operation: CommitNewFileError, rollback: FileError) -> Self {
        Self::RollbackFailed {
            operation: Box::new(operation),
            rollback: Box::new(rollback),
        }
    }
}

impl AtomicStagedFile {
    pub(crate) fn is_staging_path(path: &Path) -> bool {
        is_temp_blob_path(path)
    }

    pub async fn create(destination: &Path) -> Result<Self, FileError> {
        Self::create_with_file_sync(destination, FileSync::Enabled).await
    }

    pub(crate) async fn create_with_file_sync(
        destination: &Path,
        file_sync: FileSync,
    ) -> Result<Self, FileError> {
        let parent = destination.parent().ok_or_else(|| FileError::NoParent {
            path: destination.to_path_buf(),
        })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| FileError::at("create parent directory", parent, source))?;
        let staged = AtomicTempFile::create_in(parent)?;
        Ok(Self {
            destination: destination.to_path_buf(),
            staged: Some(staged),
            file_sync,
        })
    }

    pub fn path(&self) -> &Path {
        &self
            .staged
            .as_ref()
            .expect("atomic stage is unpublished")
            .path
    }

    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Create another unpublished stage governed by the same durability policy.
    pub async fn stage_peer(&self, destination: &Path) -> Result<Self, FileError> {
        Self::create_with_file_sync(destination, self.file_sync.clone()).await
    }

    pub async fn read_bytes(&self) -> Result<Vec<u8>, FileError> {
        tokio::fs::read(self.path())
            .await
            .map_err(|source| FileError::at("read staged blob", self.path(), source))
    }

    /// Hand the reserved path to a writer that performs its own atomic
    /// replacement. The retained descriptor is closed first so publication
    /// always names the replacement inode.
    pub fn path_for_atomic_replacement(&mut self) -> &Path {
        self.staged
            .as_mut()
            .expect("atomic stage is unpublished")
            .close();
        self.path()
    }

    pub async fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), FileError> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let staged = self.staged.as_mut().expect("atomic stage is unpublished");
        let path = staged.path.clone();
        let file = staged.file_mut();
        file.set_len(0)
            .await
            .map_err(|source| FileError::at("truncate staged blob", &path, source))?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|source| FileError::at("seek staged blob", &path, source))?;
        file.write_all(bytes)
            .await
            .map_err(|source| FileError::at("write staged blob", &path, source))?;
        // Finish Tokio's queued writes before this stage can be inspected or
        // published. The owning durability policy separately decides whether
        // the completed file also needs a physical barrier.
        self.file_sync
            .finish_async_write(file)
            .await
            .map_err(|source| FileError::at("finish staged blob write", path, source))
    }

    /// Fill this unpublished stage from a plaintext stream and apply its file
    /// durability barrier. The caller verifies higher-level content facts
    /// before publishing the stage.
    pub async fn write_plaintext<R: PlaintextChunkReader>(
        &mut self,
        source: &mut R,
    ) -> Result<u64, StreamWriteError<R::Error>> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let staged = self.staged.as_mut().expect("atomic stage is unpublished");
        let path = staged.path.clone();
        let file = staged.file_mut();
        file.set_len(0).await.map_err(|source| {
            StreamWriteError::Local(FileError::at("truncate staged blob", &path, source))
        })?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|source| {
                StreamWriteError::Local(FileError::at("seek staged blob", &path, source))
            })?;
        let mut written = 0u64;
        loop {
            let chunk = source
                .next_chunk(1 << 20)
                .await
                .map_err(StreamWriteError::Source)?;
            if chunk.is_empty() {
                break;
            }
            file.write_all(&chunk).await.map_err(|source| {
                StreamWriteError::Local(FileError::at("write staged blob", &path, source))
            })?;
            written += chunk.len() as u64;
        }
        // Finish Tokio's queued writes before the caller verifies this stage.
        // The owning durability policy separately decides whether the
        // completed file also needs a physical barrier.
        self.file_sync
            .finish_async_write(file)
            .await
            .map_err(|source| {
                StreamWriteError::Local(FileError::at("finish staged blob write", path, source))
            })?;
        Ok(written)
    }

    pub async fn write_byte_stream<E: Send>(
        mut self,
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>,
    ) -> Result<(Self, u64), ByteStreamWriteError<E>> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let write = async {
            let staged = self.staged.as_mut().expect("atomic stage is unpublished");
            let path = staged.path.clone();
            let file = staged.file_mut();
            file.set_len(0).await.map_err(|source| {
                AtomicChunkWriteError::Local(FileError::at("truncate staged blob", &path, source))
            })?;
            file.seek(std::io::SeekFrom::Start(0))
                .await
                .map_err(|source| {
                    AtomicChunkWriteError::Local(FileError::at("seek staged blob", &path, source))
                })?;
            let mut written = 0u64;
            while let Some(chunk) = stream
                .next()
                .await
                .transpose()
                .map_err(AtomicChunkWriteError::Source)?
            {
                file.write_all(&chunk).await.map_err(|source| {
                    AtomicChunkWriteError::Local(FileError::at("write staged blob", &path, source))
                })?;
                written += chunk.len() as u64;
            }
            // Finish Tokio's queued writes before this stage can be inspected
            // or published. The owning durability policy separately decides
            // whether the completed file also needs a physical barrier.
            self.file_sync
                .finish_async_write(file)
                .await
                .map_err(|source| {
                    AtomicChunkWriteError::Local(FileError::at(
                        "finish staged blob write",
                        path,
                        source,
                    ))
                })?;
            Ok(written)
        }
        .await;
        match write {
            Ok(written) => Ok((self, written)),
            Err(AtomicChunkWriteError::Source(source)) => match self.take_stage().cleanup().await {
                Ok(()) => Err(ByteStreamWriteError::Source(source)),
                Err(cleanup) => Err(ByteStreamWriteError::SourceCleanup { source, cleanup }),
            },
            Err(AtomicChunkWriteError::Local(operation)) => {
                let error = self
                    .take_stage()
                    .fail::<()>(operation)
                    .await
                    .expect_err("failed staged write returns an error");
                Err(ByteStreamWriteError::Local(error))
            }
        }
    }

    /// Fill this unpublished stage from one opened source file while computing
    /// the exact identity of the copied bytes from that same descriptor.
    pub async fn copy_from(self, source: &Path) -> Result<(Self, u64, [u8; 32]), FileError> {
        let input = tokio::fs::File::open(source)
            .await
            .map_err(|error| FileError::at("open copy source", source, error))?;
        self.write_open_file_with_facts(input, source).await
    }

    async fn write_open_file_with_facts(
        mut self,
        mut input: tokio::fs::File,
        source: &Path,
    ) -> Result<(Self, u64, [u8; 32]), FileError> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncWriteExt;

        let copy = async {
            let staged = self.staged.as_mut().expect("atomic stage is unpublished");
            let mut buffer = vec![0u8; 1 << 20];
            let mut size = 0_u64;
            let mut hasher = Sha256::new();
            loop {
                let read = input
                    .read(&mut buffer)
                    .await
                    .map_err(|error| FileError::at("read copy source", source, error))?;
                if read == 0 {
                    break;
                }
                size = size
                    .checked_add(read as u64)
                    .ok_or_else(|| FileError::SizeOverflow {
                        subject: "copy source",
                        path: source.to_path_buf(),
                    })?;
                hasher.update(&buffer[..read]);
                staged
                    .file_mut()
                    .write_all(&buffer[..read])
                    .await
                    .map_err(|error| FileError::at("write copy stage", &staged.path, error))?;
            }
            // Finish Tokio's queued writes before returning the hash and size.
            // The owning durability policy separately decides whether the
            // completed file also needs a physical barrier.
            self.file_sync
                .finish_async_write(staged.file_mut())
                .await
                .map_err(|error| FileError::at("finish copy stage", &staged.path, error))?;
            Ok::<_, FileError>((size, hasher.finalize().into()))
        }
        .await;
        match copy {
            Ok((size, hash)) => Ok((self, size, hash)),
            Err(operation) => self.take_stage().fail(operation).await,
        }
    }

    pub async fn commit(self) -> Result<(), FileError> {
        let file_sync = self.file_sync.clone();
        self.commit_with_sync(|path| {
            let path = path.to_path_buf();
            async move { file_sync.sync_parent(&path).await }
        })
        .await
    }

    async fn commit_with_sync<F, Fut>(mut self, sync_committed_parent: F) -> Result<(), FileError>
    where
        F: FnOnce(&Path) -> Fut,
        Fut: std::future::Future<Output = Result<(), FileError>>,
    {
        let mut staged = self.take_stage();
        staged.close();
        let result = async {
            tokio::fs::rename(&staged.path, &self.destination)
                .await
                .map_err(|source| {
                    FileError::between(
                        "rename verified blob",
                        &staged.path,
                        &self.destination,
                        source,
                    )
                })?;
            if let Err(operation) = sync_committed_parent(&self.destination).await {
                if let Err(source) = tokio::fs::rename(&self.destination, &staged.path).await {
                    return Err(FileError::rollback(
                        operation,
                        FileError::between(
                            "rollback verified blob rename",
                            &self.destination,
                            &staged.path,
                            source,
                        ),
                    ));
                }
                if let Err(rollback) = self.file_sync.sync_parent(&staged.path).await {
                    return Err(FileError::rollback(operation, rollback));
                }
                return Err(operation);
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                staged.disarm();
                Ok(())
            }
            Err(operation) => staged.fail(operation).await,
        }
    }

    /// Publish a verified user-owned destination without replacing an existing
    /// path. The staged file is a sibling, so the no-clobber rename exposes the
    /// complete file atomically and fails if another file already owns the name.
    pub async fn commit_new(self) -> Result<(), CommitNewFileError> {
        let file_sync = self.file_sync.clone();
        self.commit_new_with_sync(|path| {
            let path = path.to_path_buf();
            let file_sync = file_sync.clone();
            async move { file_sync.sync_parent(&path).await }
        })
        .await
    }

    async fn commit_new_with_sync<F, Fut>(
        mut self,
        mut sync_committed_parent: F,
    ) -> Result<(), CommitNewFileError>
    where
        F: FnMut(&Path) -> Fut,
        Fut: std::future::Future<Output = Result<(), FileError>>,
    {
        let mut staged = self.take_stage();
        staged.close();
        match staged.rename_noreplace(&self.destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let operation = CommitNewFileError::DestinationExists(self.destination.clone());
                return match staged.cleanup().await {
                    Ok(()) => Err(operation),
                    Err(cleanup) => Err(CommitNewFileError::rollback(operation, cleanup)),
                };
            }
            Err(source) => {
                let operation = FileError::between(
                    "rename verified blob without replacement",
                    &staged.path,
                    &self.destination,
                    source,
                );
                return match staged.cleanup().await {
                    Ok(()) => Err(operation.into()),
                    Err(cleanup) => Err(CommitNewFileError::rollback(operation.into(), cleanup)),
                };
            }
        }

        if let Err(operation) = sync_committed_parent(&self.destination).await {
            return match self.rollback_new_destination().await {
                Ok(()) => Err(operation.into()),
                Err(rollback) => Err(CommitNewFileError::rollback(operation.into(), rollback)),
            };
        }
        Ok(())
    }

    pub async fn discard(mut self) -> Result<(), FileError> {
        self.take_stage().cleanup().await
    }

    pub fn discard_blocking(mut self) -> Result<(), FileError> {
        self.take_stage().cleanup_blocking()
    }

    pub fn publish_for_transaction(mut self) -> Result<PublishedAtomicFile, FileError> {
        let staged = self.take_stage();
        staged.publish_blocking(&self.destination, &self.file_sync)?;
        Ok(PublishedAtomicFile {
            destination: self.destination.clone(),
            file_sync: self.file_sync.clone(),
        })
    }

    fn take_stage(&mut self) -> AtomicTempFile {
        self.staged.take().expect("atomic stage is unpublished")
    }

    async fn rollback_new_destination(&self) -> Result<(), FileError> {
        tokio::fs::remove_file(&self.destination)
            .await
            .map_err(|source| FileError::at("remove new destination", &self.destination, source))?;
        self.file_sync.sync_parent(&self.destination).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn write_for_test(destination: &Path, bytes: &[u8]) -> Result<(), FileError> {
        let mut staged = Self::create_with_file_sync(destination, FileSync::Disabled).await?;
        staged.write_bytes(bytes).await?;
        staged.commit().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn leave_unpublished_for_test(mut self) -> PathBuf {
        let mut staged = self.take_stage();
        let path = staged.path.clone();
        staged.disarm();
        path
    }
}

impl Drop for AtomicStagedFile {
    fn drop(&mut self) {
        // `AtomicTempFile` owns cancellation cleanup. Taking it explicitly is
        // reserved for commit, transaction publication, and reported discard.
        self.staged.take();
    }
}

impl PublishedAtomicFile {
    pub fn rollback(self) -> Result<(), FileError> {
        match std::fs::remove_file(&self.destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(FileError::at(
                    "remove published file",
                    &self.destination,
                    source,
                ));
            }
        }
        self.file_sync.sync_parent_blocking(&self.destination)
    }
}

impl AtomicTempFile {
    fn create_in(parent: &Path) -> Result<Self, FileError> {
        let named = tempfile::Builder::new()
            .prefix(TEMP_BLOB_PREFIX)
            .tempfile_in(parent)
            .map_err(|source| FileError::at("create temporary blob", parent, source))?;
        let (file, path) = named.into_parts();
        let path = path
            .keep()
            .map_err(|source| FileError::at("retain temporary blob path", parent, source.error))?;
        Ok(Self {
            path,
            file: Some(tokio::fs::File::from_std(file)),
            armed: true,
        })
    }

    fn file_mut(&mut self) -> &mut tokio::fs::File {
        self.file.as_mut().expect("atomic temp file is open")
    }

    fn close(&mut self) {
        self.file.take();
    }

    #[cfg(any(
        target_os = "android",
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "redox",
    ))]
    fn rename_noreplace(&mut self, destination: &Path) -> Result<(), std::io::Error> {
        use rustix::fs::{renameat_with, RenameFlags, CWD};

        self.close();
        renameat_with(CWD, &self.path, CWD, destination, RenameFlags::NOREPLACE)?;
        self.armed = false;
        Ok(())
    }

    #[cfg(target_os = "windows")]
    fn rename_noreplace(&mut self, destination: &Path) -> Result<(), std::io::Error> {
        self.close();
        let path = tempfile::TempPath::try_from_path(self.path.clone())?;
        match path.persist_noclobber(destination) {
            Ok(()) => {
                self.armed = false;
                Ok(())
            }
            Err(error) => {
                let source = error.error;
                // `AtomicTempFile` still owns cleanup after a failed rename.
                // Disarm the temporary guard constructed only to perform the
                // platform's no-clobber rename.
                let mut path = error.path;
                path.disable_cleanup(true);
                Err(source)
            }
        }
    }

    fn disarm(&mut self) {
        self.close();
        self.armed = false;
    }

    async fn cleanup(mut self) -> Result<(), FileError> {
        self.close();
        let cleanup = match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileError::at("remove temporary blob", &self.path, source)),
        };
        self.armed = false;
        cleanup
    }

    fn cleanup_blocking(mut self) -> Result<(), FileError> {
        self.close();
        let cleanup = match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileError::at("remove temporary blob", &self.path, source)),
        };
        self.armed = false;
        cleanup
    }

    async fn fail<T>(self, operation: FileError) -> Result<T, FileError> {
        match self.cleanup().await {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(FileError::rollback(operation, cleanup)),
        }
    }

    fn publish_blocking(
        mut self,
        destination: &Path,
        file_sync: &FileSync,
    ) -> Result<(), FileError> {
        self.close();
        let operation = match std::fs::rename(&self.path, destination) {
            Ok(()) => match file_sync.sync_parent_blocking(destination) {
                Ok(()) => {
                    self.armed = false;
                    return Ok(());
                }
                Err(operation) => {
                    let rollback = std::fs::rename(destination, &self.path)
                        .map_err(|source| {
                            FileError::between(
                                "rollback published file rename",
                                destination,
                                &self.path,
                                source,
                            )
                        })
                        .and_then(|()| file_sync.sync_parent_blocking(&self.path));
                    match rollback {
                        Ok(()) => operation,
                        Err(rollback) => FileError::rollback(operation, rollback),
                    }
                }
            },
            Err(source) => {
                FileError::between("rename temporary blob", &self.path, destination, source)
            }
        };
        match self.cleanup_blocking() {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(FileError::rollback(operation, cleanup)),
        }
    }
}

impl Drop for AtomicTempFile {
    fn drop(&mut self) {
        self.file.take();
        if !self.armed {
            return;
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "could not remove canceled atomic-write temp blob"
                );
            }
        }
    }
}

/// Size and SHA-256 digest of the file at `path`, streamed. The one
/// filesystem primitive for computing a file's identity facts; callers hold
/// the protocol reference and compare.
pub async fn file_facts(path: &Path) -> Result<(u64, [u8; 32]), FileError> {
    let (_, size, digest) =
        read_selected_with_facts(path, ExactReadSelection::IdentityOnly).await?;
    Ok((size, digest))
}

/// Size and SHA-256 digest of the file at `path`, reporting cumulative bytes
/// consumed after each read.
pub async fn file_facts_with_progress(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<(u64, [u8; 32]), FileError> {
    let (_, size, digest) =
        read_selected_with_facts_and_progress(path, ExactReadSelection::IdentityOnly, &progress)
            .await?;
    Ok((size, digest))
}

pub async fn file_len(path: &Path) -> Result<u64, FileError> {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|source| FileError::at("stat local blob", path, source))
}

#[derive(Clone, Copy)]
enum ExactReadSelection {
    IdentityOnly,
    #[cfg(test)]
    Whole,
}

async fn read_selected_with_facts(
    path: &Path,
    selection: ExactReadSelection,
) -> Result<(Vec<u8>, u64, [u8; 32]), FileError> {
    read_selected_with_facts_and_progress(path, selection, &|_| {}).await
}

async fn read_selected_with_facts_and_progress(
    path: &Path,
    selection: ExactReadSelection,
    progress: &(dyn Fn(u64) + Sync),
) -> Result<(Vec<u8>, u64, [u8; 32]), FileError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| FileError::at("open exact file", path, source))?;
    read_open_file_with_facts_and_progress(&mut file, path, selection, progress).await
}

#[cfg(test)]
async fn read_open_file_with_facts(
    file: &mut tokio::fs::File,
    path: &Path,
    selection: ExactReadSelection,
) -> Result<(Vec<u8>, u64, [u8; 32]), FileError> {
    read_open_file_with_facts_and_progress(file, path, selection, &|_| {}).await
}

async fn read_open_file_with_facts_and_progress(
    file: &mut tokio::fs::File,
    path: &Path,
    selection: ExactReadSelection,
    progress: &(dyn Fn(u64) + Sync),
) -> Result<(Vec<u8>, u64, [u8; 32]), FileError> {
    use sha2::{Digest, Sha256};

    #[cfg(test)]
    let mut selected = Vec::new();
    #[cfg(not(test))]
    let selected = Vec::new();
    let mut size = 0_u64;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 20];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| FileError::at("read exact file", path, source))?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| FileError::SizeOverflow {
                subject: "exact file",
                path: path.to_path_buf(),
            })?;
        hasher.update(&buffer[..read]);
        progress(size);
        match selection {
            ExactReadSelection::IdentityOnly => {}
            #[cfg(test)]
            ExactReadSelection::Whole => selected.extend_from_slice(&buffer[..read]),
        }
    }
    Ok((selected, size, hasher.finalize().into()))
}

/// One open file handle serving positioned reads of a local plaintext file.
///
/// Opening reads no content: a local file's current bytes are the answer to a
/// read of it, and the one place a blob's bytes are checked against the hash its
/// row declares is publication, where they become canonical synced content.
/// A read here is a read.
///
/// The handle is held for the reader's life rather than reopened per range, and
/// that is a property, not an optimization: a path can be replaced between two
/// reads, a descriptor cannot, so every range comes from the one file that was
/// opened even if it is later evicted, renamed, or replaced.
///
/// Each read positions the descriptor itself, and the mutex makes that seek and
/// its read one operation, so concurrent readers of one handle cannot interleave
/// into each other's ranges.
pub struct OpenFile {
    file: tokio::sync::Mutex<tokio::fs::File>,
    path: PathBuf,
    size: u64,
}

impl OpenFile {
    /// Open `path` and stat it for the length its reads are bounded by.
    pub async fn open(path: &Path) -> Result<Self, FileError> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|source| FileError::at("open local file", path, source))?;
        let size = file
            .metadata()
            .await
            .map_err(|source| FileError::at("stat local file", path, source))?
            .len();
        Ok(Self {
            file: tokio::sync::Mutex::new(file),
            path: path.to_path_buf(),
            size,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Read exactly `len` bytes at `offset`. The caller bounds the range against
    /// [`size`](Self::size); a file that cannot supply them is an error, never a
    /// short result.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileError> {
        use tokio::io::AsyncSeekExt;

        if len == 0 {
            return Ok(Vec::new());
        }
        let mut buffer =
            vec![0_u8; usize::try_from(len).map_err(|_| FileError::RangeTooLarge { len })?];
        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| FileError::at("seek local file range", &self.path, source))?;
        file.read_exact(&mut buffer)
            .await
            .map_err(|source| FileError::at("read local file range", &self.path, source))?;
        Ok(buffer)
    }
}

#[cfg(test)]
pub(crate) async fn read(path: &Path) -> Result<Vec<u8>, FileError> {
    tokio::fs::read(path)
        .await
        .map_err(|source| FileError::at("read local blob", path, source))
}

#[cfg(test)]
pub(crate) async fn exists(path: &Path) -> Result<bool, FileError> {
    tokio::fs::try_exists(path)
        .await
        .map_err(|source| FileError::at("check local blob", path, source))
}

#[cfg(test)]
pub(crate) async fn rename(from: &Path, to: &Path) -> Result<(), FileError> {
    tokio::fs::rename(from, to)
        .await
        .map_err(|source| FileError::between("rename file", from, to, source))
}

#[cfg(test)]
pub(crate) async fn remove_file(path: &Path) -> Result<bool, FileError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(FileError::at("remove file", path, source)),
    }
}

#[cfg(test)]
#[path = "local_file_tests.rs"]
mod tests;
