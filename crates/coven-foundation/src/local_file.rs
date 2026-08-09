//! Private local-file machinery used by storage and store-directory capabilities.

use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncReadExt;

use crate::atomic_file::FileSync;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamWriteError<E> {
    Source(E),
    Local(String),
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
    SourceCleanup { source: E, cleanup: String },
    Local(String),
}

#[async_trait]
pub trait PlaintextChunkReader: Send {
    type Error: Send;
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, Self::Error>;
}

enum AtomicChunkWriteError<E> {
    Source(E),
    Local(String),
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

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CommitNewFileError {
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("commit new file: {0}")]
    Filesystem(String),
    #[error("{operation}; rollback failed: {rollback}")]
    RollbackFailed { operation: String, rollback: String },
}

impl AtomicStagedFile {
    pub(crate) fn is_staging_path(path: &Path) -> bool {
        is_temp_blob_path(path)
    }

    pub async fn create(destination: &Path) -> Result<Self, String> {
        Self::create_with_file_sync(destination, FileSync::Enabled).await
    }

    pub(crate) async fn create_with_file_sync(
        destination: &Path,
        file_sync: FileSync,
    ) -> Result<Self, String> {
        let parent = destination
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", destination.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create parent dir for {}: {error}", destination.display()))?;
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
    pub async fn stage_peer(&self, destination: &Path) -> Result<Self, String> {
        Self::create_with_file_sync(destination, self.file_sync.clone()).await
    }

    pub async fn read_bytes(&self) -> Result<Vec<u8>, String> {
        tokio::fs::read(self.path())
            .await
            .map_err(|error| format!("read staged blob {}: {error}", self.path().display()))
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

    pub async fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let staged = self.staged.as_mut().expect("atomic stage is unpublished");
        let path = staged.path.clone();
        let file = staged.file_mut();
        file.set_len(0)
            .await
            .map_err(|error| format!("truncate staged blob {}: {error}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|error| format!("seek staged blob {}: {error}", path.display()))?;
        file.write_all(bytes)
            .await
            .map_err(|error| format!("write staged blob {}: {error}", path.display()))?;
        // This writes an already-open blob stage that a later rename publishes,
        // so its owning durability policy handles the completed file here and
        // the committed parent directory at publication.
        self.file_sync
            .sync_file(file)
            .await
            .map_err(|error| format!("fsync staged blob {}: {error}", path.display()))
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
        file.set_len(0).await.map_err(|error| {
            StreamWriteError::Local(format!("truncate staged blob {}: {error}", path.display()))
        })?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|error| {
                StreamWriteError::Local(format!("seek staged blob {}: {error}", path.display()))
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
            file.write_all(&chunk).await.map_err(|error| {
                StreamWriteError::Local(format!("write staged blob {}: {error}", path.display()))
            })?;
            written += chunk.len() as u64;
        }
        // The payload arrives as a stream of chunks, so its owning durability
        // policy handles the completed file here and the committed parent
        // directory at publication.
        self.file_sync.sync_file(file).await.map_err(|error| {
            StreamWriteError::Local(format!("fsync staged blob {}: {error}", path.display()))
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
            file.set_len(0).await.map_err(|error| {
                AtomicChunkWriteError::Local(format!(
                    "truncate staged blob {}: {error}",
                    path.display()
                ))
            })?;
            file.seek(std::io::SeekFrom::Start(0))
                .await
                .map_err(|error| {
                    AtomicChunkWriteError::Local(format!(
                        "seek staged blob {}: {error}",
                        path.display()
                    ))
                })?;
            let mut written = 0u64;
            while let Some(chunk) = stream
                .next()
                .await
                .transpose()
                .map_err(AtomicChunkWriteError::Source)?
            {
                file.write_all(&chunk).await.map_err(|error| {
                    AtomicChunkWriteError::Local(format!(
                        "write staged blob {}: {error}",
                        path.display()
                    ))
                })?;
                written += chunk.len() as u64;
            }
            // The stage is filled incrementally, so its owning durability
            // policy handles the completed file here and the committed parent
            // directory at publication.
            self.file_sync.sync_file(file).await.map_err(|error| {
                AtomicChunkWriteError::Local(format!(
                    "fsync staged blob {}: {error}",
                    path.display()
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
    pub async fn copy_from(self, source: &Path) -> Result<(Self, u64, [u8; 32]), String> {
        let input = tokio::fs::File::open(source)
            .await
            .map_err(|error| format!("open copy source {}: {error}", source.display()))?;
        self.write_open_file_with_facts(input, source).await
    }

    async fn write_open_file_with_facts(
        mut self,
        mut input: tokio::fs::File,
        source: &Path,
    ) -> Result<(Self, u64, [u8; 32]), String> {
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
                    .map_err(|error| format!("read pin source {}: {error}", source.display()))?;
                if read == 0 {
                    break;
                }
                size = size
                    .checked_add(read as u64)
                    .ok_or_else(|| format!("copy source size overflow: {}", source.display()))?;
                hasher.update(&buffer[..read]);
                staged
                    .file_mut()
                    .write_all(&buffer[..read])
                    .await
                    .map_err(|error| {
                        format!("write temp pin {}: {error}", staged.path.display())
                    })?;
            }
            // This copies through a fixed buffer while hashing, so the owning
            // durability policy handles the completed file here and the
            // committed parent directory at publication.
            self.file_sync
                .sync_file(staged.file_mut())
                .await
                .map_err(|error| format!("fsync temp pin {}: {error}", staged.path.display()))?;
            Ok::<_, String>((size, hasher.finalize().into()))
        }
        .await;
        match copy {
            Ok((size, hash)) => Ok((self, size, hash)),
            Err(operation) => self.take_stage().fail(operation).await,
        }
    }

    pub async fn commit(self) -> Result<(), String> {
        let file_sync = self.file_sync.clone();
        self.commit_with_sync(|path| {
            let path = path.to_path_buf();
            async move { file_sync.sync_parent(&path).await }
        })
        .await
    }

    async fn commit_with_sync<F, Fut>(mut self, sync_committed_parent: F) -> Result<(), String>
    where
        F: FnOnce(&Path) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let mut staged = self.take_stage();
        staged.close();
        let result = async {
            tokio::fs::rename(&staged.path, &self.destination)
                .await
                .map_err(|error| {
                    format!(
                        "rename verified blob {} -> {}: {error}",
                        staged.path.display(),
                        self.destination.display()
                    )
                })?;
            if let Err(operation) = sync_committed_parent(&self.destination).await {
                tokio::fs::rename(&self.destination, &staged.path)
                    .await
                    .map_err(|rollback| {
                        format!(
                            "{operation}; rollback rename {} -> {} failed: {rollback}",
                            self.destination.display(),
                            staged.path.display()
                        )
                    })?;
                self.file_sync
                    .sync_parent(&staged.path)
                    .await
                    .map_err(|rollback| {
                        format!("{operation}; rollback directory sync failed: {rollback}")
                    })?;
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
    /// path. The staged file is a sibling, so the hard link exposes one complete
    /// inode atomically and fails if another file already owns the name.
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
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let mut staged = self.take_stage();
        staged.close();
        match tokio::fs::hard_link(&staged.path, &self.destination).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let operation = CommitNewFileError::DestinationExists(self.destination.clone());
                return match staged.cleanup().await {
                    Ok(()) => Err(operation),
                    Err(cleanup) => Err(CommitNewFileError::RollbackFailed {
                        operation: operation.to_string(),
                        rollback: cleanup,
                    }),
                };
            }
            Err(error) => {
                let operation = format!(
                    "link verified blob {} -> {}: {error}",
                    staged.path.display(),
                    self.destination.display()
                );
                return match staged.cleanup().await {
                    Ok(()) => Err(CommitNewFileError::Filesystem(operation)),
                    Err(cleanup) => Err(CommitNewFileError::RollbackFailed {
                        operation,
                        rollback: cleanup,
                    }),
                };
            }
        }

        if let Err(operation) = sync_committed_parent(&self.destination).await {
            let mut failures = Vec::new();
            if let Err(error) = self.rollback_new_destination(&operation).await {
                failures.push(error.to_string());
            }
            if let Err(error) = staged.cleanup().await {
                failures.push(error);
            }
            return if failures.is_empty() {
                Err(CommitNewFileError::Filesystem(operation))
            } else {
                Err(CommitNewFileError::RollbackFailed {
                    operation,
                    rollback: failures.join("; "),
                })
            };
        }
        if let Err(operation) = staged.cleanup().await {
            self.rollback_new_destination(&operation).await?;
            return Err(CommitNewFileError::Filesystem(operation));
        }
        if let Err(operation) = sync_committed_parent(&self.destination).await {
            self.rollback_new_destination(&operation).await?;
            return Err(CommitNewFileError::Filesystem(operation));
        }
        Ok(())
    }

    pub async fn discard(mut self) -> Result<(), String> {
        self.take_stage().cleanup().await
    }

    pub fn discard_blocking(mut self) -> Result<(), String> {
        self.take_stage().cleanup_blocking()
    }

    pub fn publish_for_transaction(mut self) -> Result<PublishedAtomicFile, String> {
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

    async fn rollback_new_destination(&self, operation: &str) -> Result<(), CommitNewFileError> {
        tokio::fs::remove_file(&self.destination)
            .await
            .map_err(|error| CommitNewFileError::RollbackFailed {
                operation: operation.to_string(),
                rollback: format!(
                    "remove new destination {}: {error}",
                    self.destination.display()
                ),
            })?;
        self.file_sync
            .sync_parent(&self.destination)
            .await
            .map_err(|rollback| CommitNewFileError::RollbackFailed {
                operation: operation.to_string(),
                rollback,
            })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn write_for_test(destination: &Path, bytes: &[u8]) -> Result<(), String> {
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
    pub fn rollback(self) -> Result<(), String> {
        match std::fs::remove_file(&self.destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "remove published file {}: {error}",
                    self.destination.display()
                ));
            }
        }
        self.file_sync.sync_parent_blocking(&self.destination)
    }
}

impl AtomicTempFile {
    fn create_in(parent: &Path) -> Result<Self, String> {
        let named = tempfile::Builder::new()
            .prefix(TEMP_BLOB_PREFIX)
            .tempfile_in(parent)
            .map_err(|error| {
                format!("create temporary blob under {}: {error}", parent.display())
            })?;
        let (file, path) = named.into_parts();
        let path = path
            .keep()
            .map_err(|error| format!("retain temporary blob path: {error}"))?;
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

    fn disarm(&mut self) {
        self.close();
        self.armed = false;
    }

    async fn cleanup(mut self) -> Result<(), String> {
        self.close();
        let cleanup = match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!(
                "remove temporary blob {}: {error}",
                self.path.display()
            )),
        };
        self.armed = false;
        cleanup
    }

    fn cleanup_blocking(mut self) -> Result<(), String> {
        self.close();
        let cleanup = match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!(
                "remove temporary blob {}: {error}",
                self.path.display()
            )),
        };
        self.armed = false;
        cleanup
    }

    async fn fail<T>(self, operation: String) -> Result<T, String> {
        match self.cleanup().await {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(format!("{operation}; {cleanup}")),
        }
    }

    fn publish_blocking(mut self, destination: &Path, file_sync: &FileSync) -> Result<(), String> {
        self.close();
        let operation = match std::fs::rename(&self.path, destination) {
            Ok(()) => match file_sync.sync_parent_blocking(destination) {
                Ok(()) => {
                    self.armed = false;
                    return Ok(());
                }
                Err(operation) => {
                    let rollback = std::fs::rename(destination, &self.path)
                        .map_err(|error| {
                            format!(
                                "rollback rename {} -> {}: {error}",
                                destination.display(),
                                self.path.display()
                            )
                        })
                        .and_then(|()| file_sync.sync_parent_blocking(&self.path));
                    match rollback {
                        Ok(()) => operation,
                        Err(rollback) => format!("{operation}; {rollback}"),
                    }
                }
            },
            Err(error) => format!(
                "rename temporary blob {} -> {}: {error}",
                self.path.display(),
                destination.display()
            ),
        };
        match self.cleanup_blocking() {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(format!("{operation}; {cleanup}")),
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
pub async fn file_facts(path: &Path) -> Result<(u64, [u8; 32]), String> {
    let (_, size, digest) =
        read_selected_with_facts(path, ExactReadSelection::IdentityOnly).await?;
    Ok((size, digest))
}

pub async fn file_len(path: &Path) -> Result<u64, String> {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|e| format!("stat local blob {}: {e}", path.display()))
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
) -> Result<(Vec<u8>, u64, [u8; 32]), String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| format!("open exact file {}: {error}", path.display()))?;
    read_open_file_with_facts(&mut file, path, selection).await
}

async fn read_open_file_with_facts(
    file: &mut tokio::fs::File,
    path: &Path,
    selection: ExactReadSelection,
) -> Result<(Vec<u8>, u64, [u8; 32]), String> {
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
            .map_err(|error| format!("read exact file {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| format!("exact file size overflow: {}", path.display()))?;
        hasher.update(&buffer[..read]);
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
    pub async fn open(path: &Path) -> Result<Self, String> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| format!("open local file {}: {error}", path.display()))?;
        let size = file
            .metadata()
            .await
            .map_err(|error| format!("stat local file {}: {error}", path.display()))?
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
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        use tokio::io::AsyncSeekExt;

        if len == 0 {
            return Ok(Vec::new());
        }
        let mut buffer = vec![
            0_u8;
            usize::try_from(len).map_err(|_| format!(
                "local file range is too large: {len} bytes"
            ))?
        ];
        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|error| {
                format!(
                    "seek local file {} to {offset}: {error}",
                    self.path.display()
                )
            })?;
        file.read_exact(&mut buffer).await.map_err(|error| {
            format!(
                "read {len} bytes at {offset} from local file {}: {error}",
                self.path.display()
            )
        })?;
        Ok(buffer)
    }
}

#[cfg(test)]
pub(crate) async fn read(path: &Path) -> Result<Vec<u8>, String> {
    tokio::fs::read(path)
        .await
        .map_err(|e| format!("read local blob {}: {e}", path.display()))
}

#[cfg(test)]
pub(crate) async fn exists(path: &Path) -> Result<bool, String> {
    tokio::fs::try_exists(path)
        .await
        .map_err(|e| format!("check local blob {}: {e}", path.display()))
}

#[cfg(test)]
pub(crate) async fn rename(from: &Path, to: &Path) -> Result<(), String> {
    tokio::fs::rename(from, to)
        .await
        .map_err(|e| format!("rename {} -> {}: {e}", from.display(), to.display()))
}

#[cfg(test)]
pub(crate) async fn remove_file(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove file {}: {error}", path.display())),
    }
}

#[cfg(test)]
#[path = "local_file_tests.rs"]
mod tests;
