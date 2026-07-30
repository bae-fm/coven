use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncReadExt;

/// The filename prefix an atomic blob write gives its in-progress temp sibling
/// (`.tmp.<uuid>`) before the fsync-then-rename that makes it the committed
/// destination.
pub(crate) const TEMP_BLOB_PREFIX: &str = ".tmp.";

/// Whether `path`'s file name marks it as an atomic-write temp sibling.
pub(crate) fn is_temp_blob_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(TEMP_BLOB_PREFIX))
}

pub(crate) struct PlaintextReader(Box<dyn PlaintextChunkReader>);

impl PlaintextReader {
    pub(crate) async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        self.0
            .next_chunk(max)
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn from_test_reader(reader: impl PlaintextChunkReader + 'static) -> Self {
        Self(Box::new(reader))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PlaintextChunkError {
    #[error(transparent)]
    Remote(#[from] crate::storage::StorageError),
    #[error("invalid remote content: {0}")]
    InvalidContent(String),
    #[error("local plaintext source: {0}")]
    Local(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StreamWriteError {
    #[error(transparent)]
    Source(#[from] PlaintextChunkError),
    #[error("local destination: {0}")]
    Local(String),
}

#[derive(Debug)]
pub(crate) enum ByteStreamWriteError<E> {
    Source(E),
    SourceCleanup { source: E, cleanup: String },
    Local(String),
}

#[async_trait]
pub(crate) trait PlaintextChunkReader: Send {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, PlaintextChunkError>;
}

enum AtomicChunkWriteError<E> {
    Source(E),
    SourceCleanup { source: E, cleanup: String },
    Local(String),
}

struct AtomicTempFile {
    path: PathBuf,
    file: Option<tokio::fs::File>,
    armed: bool,
}

/// A provider download path that becomes visible at its destination only after
/// the caller has verified the completed file.
pub(crate) struct AtomicStagedFile {
    destination: PathBuf,
    staged: Option<AtomicTempFile>,
}

/// A staged file that has been installed at its destination while the caller's
/// durable transaction is still deciding whether that installation commits.
pub(crate) struct PublishedAtomicFile {
    destination: PathBuf,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CommitNewFileError {
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("commit new file: {0}")]
    Filesystem(String),
    #[error("{operation}; rollback failed: {rollback}")]
    RollbackFailed { operation: String, rollback: String },
}

impl AtomicStagedFile {
    pub(crate) async fn create(destination: &Path) -> Result<Self, String> {
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
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self
            .staged
            .as_ref()
            .expect("atomic stage is unpublished")
            .path
    }

    /// Hand the reserved path to a writer that performs its own atomic
    /// replacement. The retained descriptor is closed first so publication
    /// always names the replacement inode.
    pub(crate) fn path_for_atomic_replacement(&mut self) -> &Path {
        self.staged
            .as_mut()
            .expect("atomic stage is unpublished")
            .close();
        self.path()
    }

    pub(crate) async fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
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
        file.sync_all()
            .await
            .map_err(|error| format!("fsync staged blob {}: {error}", path.display()))
    }

    /// Fill this unpublished stage from a plaintext stream and fsync the
    /// completed file. The caller verifies higher-level content facts before
    /// publishing the stage.
    pub(crate) async fn write_plaintext(
        &mut self,
        source: &mut dyn PlaintextChunkReader,
    ) -> Result<u64, StreamWriteError> {
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
            let chunk = source.next_chunk(1 << 20).await?;
            if chunk.is_empty() {
                break;
            }
            file.write_all(&chunk).await.map_err(|error| {
                StreamWriteError::Local(format!("write staged blob {}: {error}", path.display()))
            })?;
            written += chunk.len() as u64;
        }
        file.sync_all().await.map_err(|error| {
            StreamWriteError::Local(format!("fsync staged blob {}: {error}", path.display()))
        })?;
        Ok(written)
    }

    pub(crate) async fn write_byte_stream<E: Send>(
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
            file.sync_all().await.map_err(|error| {
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
            Err(AtomicChunkWriteError::SourceCleanup { .. }) => {
                unreachable!("source cleanup errors are assembled after writing")
            }
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

    pub(crate) async fn commit(mut self) -> Result<(), String> {
        let mut staged = self.take_stage();
        staged.close();
        let result = commit_staged_file(&staged.path, &self.destination, |path| {
            let path = path.to_path_buf();
            async move { sync_parent_dir(&path).await }
        })
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
    pub(crate) async fn commit_new(self) -> Result<(), CommitNewFileError> {
        self.commit_new_with_sync(|path| {
            let path = path.to_path_buf();
            async move { sync_parent_dir(&path).await }
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
            if let Err(error) = rollback_new_destination(&self.destination, &operation).await {
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
            rollback_new_destination(&self.destination, &operation).await?;
            return Err(CommitNewFileError::Filesystem(operation));
        }
        if let Err(operation) = sync_committed_parent(&self.destination).await {
            rollback_new_destination(&self.destination, &operation).await?;
            return Err(CommitNewFileError::Filesystem(operation));
        }
        Ok(())
    }

    pub(crate) async fn discard(mut self) -> Result<(), String> {
        self.take_stage().cleanup().await
    }

    pub(crate) fn discard_blocking(mut self) -> Result<(), String> {
        self.take_stage().cleanup_blocking()
    }

    pub(crate) fn publish_for_transaction(mut self) -> Result<PublishedAtomicFile, String> {
        let staged = self.take_stage();
        staged.publish_blocking(&self.destination)?;
        Ok(PublishedAtomicFile {
            destination: self.destination.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn leave_unpublished_for_test(mut self) -> PathBuf {
        let mut staged = self.take_stage();
        let path = staged.path.clone();
        staged.disarm();
        path
    }

    fn take_stage(&mut self) -> AtomicTempFile {
        self.staged.take().expect("atomic stage is unpublished")
    }
}

async fn commit_staged_file<F, Fut>(
    staged: &Path,
    destination: &Path,
    sync_committed_parent: F,
) -> Result<(), String>
where
    F: FnOnce(&Path) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    tokio::fs::rename(staged, destination)
        .await
        .map_err(|error| {
            format!(
                "rename verified blob {} -> {}: {error}",
                staged.display(),
                destination.display()
            )
        })?;
    if let Err(operation) = sync_committed_parent(destination).await {
        tokio::fs::rename(destination, staged)
            .await
            .map_err(|rollback| {
                format!(
                    "{operation}; rollback rename {} -> {} failed: {rollback}",
                    destination.display(),
                    staged.display()
                )
            })?;
        sync_parent_dir(staged).await.map_err(|rollback| {
            format!("{operation}; rollback directory sync failed: {rollback}")
        })?;
        return Err(operation);
    }
    Ok(())
}

async fn rollback_new_destination(
    destination: &Path,
    operation: &str,
) -> Result<(), CommitNewFileError> {
    tokio::fs::remove_file(destination).await.map_err(|error| {
        CommitNewFileError::RollbackFailed {
            operation: operation.to_string(),
            rollback: format!("remove new destination {}: {error}", destination.display()),
        }
    })?;
    sync_parent_dir(destination)
        .await
        .map_err(|rollback| CommitNewFileError::RollbackFailed {
            operation: operation.to_string(),
            rollback,
        })
}

impl Drop for AtomicStagedFile {
    fn drop(&mut self) {
        // `AtomicTempFile` owns cancellation cleanup. Taking it explicitly is
        // reserved for commit, transaction publication, and reported discard.
        self.staged.take();
    }
}

impl PublishedAtomicFile {
    pub(crate) fn rollback(self) -> Result<(), String> {
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
        sync_parent_dir_blocking(&self.destination)
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

    async fn commit(mut self, destination: &Path) -> Result<(), String> {
        self.close();
        if let Err(error) = tokio::fs::rename(&self.path, destination).await {
            let operation = format!(
                "rename temporary blob {} -> {}: {error}",
                self.path.display(),
                destination.display()
            );
            return self.fail(operation).await;
        }
        self.armed = false;
        Ok(())
    }

    fn publish_blocking(mut self, destination: &Path) -> Result<(), String> {
        self.close();
        let operation = match std::fs::rename(&self.path, destination) {
            Ok(()) => match sync_parent_dir_blocking(destination) {
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
                        .and_then(|()| sync_parent_dir_blocking(&self.path));
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

struct FilePlaintextReader {
    file: tokio::fs::File,
    path: PathBuf,
}

#[async_trait]
impl PlaintextChunkReader for FilePlaintextReader {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, PlaintextChunkError> {
        debug_assert!(max > 0, "next_chunk max must be positive");
        let mut buf = vec![0u8; max];
        let mut filled = 0;
        while filled < max {
            let read = self.file.read(&mut buf[filled..]).await.map_err(|e| {
                PlaintextChunkError::Local(format!("read local blob {}: {e}", self.path.display()))
            })?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        buf.truncate(filled);
        Ok(buf)
    }
}

pub(crate) async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open local blob {} for streaming: {e}", path.display()))?;
    Ok(PlaintextReader(Box::new(FilePlaintextReader {
        file,
        path: path.to_path_buf(),
    })))
}

pub(crate) async fn file_len(path: &Path) -> Result<u64, String> {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|e| format!("stat local blob {}: {e}", path.display()))
}

/// Stream the exact stored size and SHA-256 identity of a local file.
pub(crate) async fn exact_file_facts(
    path: &Path,
) -> Result<(u64, crate::protocol::store_commit::ObjectHash), String> {
    let (_, size, hash) = read_selected_with_facts(path, ExactReadSelection::IdentityOnly).await?;
    Ok((size, hash))
}

#[derive(Clone, Copy)]
enum ExactReadSelection {
    IdentityOnly,
    Whole,
}

/// Read bytes and calculate their whole-file identity through one open file
/// handle. Replacing the path during the read cannot make the returned bytes come
/// from a different inode than the size and hash.
pub(crate) async fn read_with_facts(
    path: &Path,
) -> Result<(Vec<u8>, u64, crate::protocol::store_commit::ObjectHash), String> {
    read_selected_with_facts(path, ExactReadSelection::Whole).await
}

async fn read_selected_with_facts(
    path: &Path,
    selection: ExactReadSelection,
) -> Result<(Vec<u8>, u64, crate::protocol::store_commit::ObjectHash), String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| format!("open exact file {}: {error}", path.display()))?;
    read_open_file_with_facts(&mut file, path, selection).await
}

async fn read_open_file_with_facts(
    file: &mut tokio::fs::File,
    path: &Path,
    selection: ExactReadSelection,
) -> Result<(Vec<u8>, u64, crate::protocol::store_commit::ObjectHash), String> {
    use sha2::{Digest, Sha256};

    let mut selected = Vec::new();
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
            ExactReadSelection::Whole => selected.extend_from_slice(&buffer[..read]),
        }
    }
    Ok((
        selected,
        size,
        crate::protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into()),
    ))
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
pub(crate) struct OpenFile {
    file: tokio::sync::Mutex<tokio::fs::File>,
    path: PathBuf,
    size: u64,
}

impl OpenFile {
    /// Open `path` and stat it for the length its reads are bounded by.
    pub(crate) async fn open(path: &Path) -> Result<Self, String> {
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

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    /// Read exactly `len` bytes at `offset`. The caller bounds the range against
    /// [`size`](Self::size); a file that cannot supply them is an error, never a
    /// short result.
    pub(crate) async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, String> {
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

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ExactFileVerificationError {
    #[error("inspect exact file: {0}")]
    Filesystem(String),
    #[error("{0}")]
    IdentityMismatch(String),
}

pub(crate) async fn verify_exact_file(
    object: &crate::storage::ExactObjectRef,
    path: &Path,
) -> Result<(), ExactFileVerificationError> {
    let (size, hash) = exact_file_facts(path)
        .await
        .map_err(ExactFileVerificationError::Filesystem)?;
    if size != object.stored_size() || hash != object.stored_hash() {
        return Err(ExactFileVerificationError::IdentityMismatch(format!(
            "exact file {} does not match stored identity for {}",
            path.display(),
            object.slot().logical_key()
        )));
    }
    Ok(())
}

pub(crate) async fn copy_atomic(src: &Path, dst: &Path) -> Result<(), String> {
    copy_atomic_with_facts(src, dst).await.map(|_| ())
}

/// Copy one file atomically while calculating the exact identity of the bytes
/// written from the same open source handle.
pub(crate) async fn copy_atomic_with_facts(
    src: &Path,
    dst: &Path,
) -> Result<(u64, crate::protocol::store_commit::ObjectHash), String> {
    let input = tokio::fs::File::open(src)
        .await
        .map_err(|error| format!("open copy source {}: {error}", src.display()))?;
    copy_open_file_atomic_with_facts(input, src, dst).await
}

async fn copy_open_file_atomic_with_facts(
    mut input: tokio::fs::File,
    src: &Path,
    dst: &Path,
) -> Result<(u64, crate::protocol::store_commit::ObjectHash), String> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;

    let parent = dst
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", dst.display()))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|e| format!("create parent dir for {}: {e}", dst.display()))?;
    let mut temp = AtomicTempFile::create_in(parent)?;

    let copy = async {
        let mut buf = vec![0u8; 1 << 20];
        let mut size = 0_u64;
        let mut hasher = Sha256::new();
        loop {
            let read = input
                .read(&mut buf)
                .await
                .map_err(|e| format!("read pin source {}: {e}", src.display()))?;
            if read == 0 {
                break;
            }
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| format!("copy source size overflow: {}", src.display()))?;
            hasher.update(&buf[..read]);
            temp.file_mut()
                .write_all(&buf[..read])
                .await
                .map_err(|e| format!("write temp pin {}: {e}", temp.path.display()))?;
        }
        temp.file_mut()
            .sync_all()
            .await
            .map_err(|e| format!("fsync temp pin {}: {e}", temp.path.display()))?;
        Ok::<_, String>((
            size,
            crate::protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into()),
        ))
    }
    .await;
    let facts = match copy {
        Ok(facts) => facts,
        Err(error) => {
            return temp.fail(error).await;
        }
    };
    temp.commit(dst).await?;
    Ok(facts)
}

pub(crate) async fn write_byte_stream_atomic<E: Send>(
    path: &Path,
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>,
) -> Result<u64, ByteStreamWriteError<E>> {
    write_atomic_chunks(path, stream)
        .await
        .map_err(|error| match error {
            AtomicChunkWriteError::Source(error) => ByteStreamWriteError::Source(error),
            AtomicChunkWriteError::SourceCleanup { source, cleanup } => {
                ByteStreamWriteError::SourceCleanup { source, cleanup }
            }
            AtomicChunkWriteError::Local(error) => ByteStreamWriteError::Local(error),
        })
}

async fn write_atomic_chunks<E: Send>(
    path: &Path,
    mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>,
) -> Result<u64, AtomicChunkWriteError<E>> {
    use tokio::io::AsyncWriteExt;

    let parent = path.parent().ok_or_else(|| {
        AtomicChunkWriteError::Local(format!("blob path has no parent dir: {}", path.display()))
    })?;
    tokio::fs::create_dir_all(parent).await.map_err(|e| {
        AtomicChunkWriteError::Local(format!("create parent dir for {}: {e}", path.display()))
    })?;
    let mut temp = AtomicTempFile::create_in(parent).map_err(AtomicChunkWriteError::Local)?;
    let write = async {
        let mut written = 0u64;
        while let Some(chunk) = stream
            .next()
            .await
            .transpose()
            .map_err(AtomicChunkWriteError::Source)?
        {
            let temp_path = temp.path.clone();
            temp.file_mut().write_all(&chunk).await.map_err(|e| {
                AtomicChunkWriteError::Local(format!(
                    "write temp blob {}: {e}",
                    temp_path.display()
                ))
            })?;
            written += chunk.len() as u64;
        }
        let temp_path = temp.path.clone();
        temp.file_mut().sync_all().await.map_err(|e| {
            AtomicChunkWriteError::Local(format!("fsync temp blob {}: {e}", temp_path.display()))
        })?;
        Ok(written)
    }
    .await;
    let written = match write {
        Ok(written) => written,
        Err(AtomicChunkWriteError::Source(source)) => {
            return match temp.cleanup().await {
                Ok(()) => Err(AtomicChunkWriteError::Source(source)),
                Err(cleanup) => Err(AtomicChunkWriteError::SourceCleanup { source, cleanup }),
            };
        }
        Err(AtomicChunkWriteError::SourceCleanup { .. }) => {
            unreachable!("source cleanup errors are assembled after writing")
        }
        Err(AtomicChunkWriteError::Local(operation)) => {
            let error = temp
                .fail::<()>(operation)
                .await
                .expect_err("failed temporary write returns an error");
            return Err(AtomicChunkWriteError::Local(error));
        }
    };
    temp.commit(path)
        .await
        .map_err(AtomicChunkWriteError::Local)?;
    Ok(written)
}

pub(crate) async fn read(path: &Path) -> Result<Vec<u8>, String> {
    tokio::fs::read(path)
        .await
        .map_err(|e| format!("read local blob {}: {e}", path.display()))
}

#[cfg(test)]
pub(crate) async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let parent = path
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
    let mut temp = AtomicTempFile::create_in(parent)?;

    let write = async {
        let temp_path = temp.path.clone();
        temp.file_mut()
            .write_all(bytes)
            .await
            .map_err(|e| format!("write temp blob {}: {e}", temp_path.display()))?;
        temp.file_mut()
            .sync_all()
            .await
            .map_err(|e| format!("fsync temp blob {}: {e}", temp_path.display()))
    }
    .await;
    if let Err(error) = write {
        return temp.fail(error).await;
    }
    temp.commit(path).await
}

/// Write atomically and fsync the parent directory containing the committed name.
#[cfg(test)]
pub(crate) async fn write_atomic_durable(path: &Path, bytes: &[u8]) -> Result<(), String> {
    write_atomic(path, bytes).await?;
    sync_parent_dir(path).await
}

/// Blocking atomic file installation for synchronous persistence boundaries.
/// The temp file is a unique sibling, so the rename cannot cross filesystems.
pub(crate) fn write_atomic_durable_blocking(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;

    let parent = path
        .parent()
        .ok_or_else(|| format!("path has no parent directory: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create parent directory {}: {error}", parent.display()))?;
    let named = tempfile::Builder::new()
        .prefix(TEMP_BLOB_PREFIX)
        .tempfile_in(parent)
        .map_err(|error| format!("create temporary file under {}: {error}", parent.display()))?;
    let (mut file, temp) = named.into_parts();
    let temp = temp
        .keep()
        .map_err(|error| format!("retain temporary file path: {error}"))?;
    let write = (|| {
        file.write_all(bytes)
            .map_err(|error| format!("write temp file {}: {error}", temp.display()))?;
        file.sync_all()
            .map_err(|error| format!("fsync temp file {}: {error}", temp.display()))?;
        drop(file);
        std::fs::rename(&temp, path).map_err(|error| {
            format!(
                "rename temp file {} to {}: {error}",
                temp.display(),
                path.display()
            )
        })?;
        std::fs::File::open(parent)
            .map_err(|error| format!("open parent directory {}: {error}", parent.display()))?
            .sync_all()
            .map_err(|error| format!("fsync parent directory {}: {error}", parent.display()))
    })();
    if let Err(operation) = write {
        return match std::fs::remove_file(&temp) {
            Ok(()) => Err(operation),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(operation),
            Err(cleanup) => Err(format!(
                "{operation}; remove failed temp file {}: {cleanup}",
                temp.display()
            )),
        };
    }
    Ok(())
}

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

pub(crate) async fn remove_file(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove file {}: {error}", path.display())),
    }
}

#[cfg(test)]
pub(crate) async fn remove_dir_all(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove dir tree {}: {error}", path.display())),
    }
}

pub(crate) async fn create_dir_all(path: &Path) -> Result<(), String> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|e| format!("create dir tree {}: {e}", path.display()))
}

pub(crate) async fn sync_parent_dir(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("path has no parent dir: {}", path.display()))?;
    let dir = tokio::fs::File::open(parent)
        .await
        .map_err(|e| format!("open parent dir {} to fsync: {e}", parent.display()))?;
    dir.sync_all()
        .await
        .map_err(|e| format!("fsync parent dir {}: {e}", parent.display()))
}

fn sync_parent_dir_blocking(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("path has no parent dir: {}", path.display()))?;
    let dir = std::fs::File::open(parent)
        .map_err(|error| format!("open parent dir {} to fsync: {error}", parent.display()))?;
    dir.sync_all()
        .map_err(|error| format!("fsync parent dir {}: {error}", parent.display()))
}

/// Every committed file under `dir` as `(path, mtime_ms, size)`.
pub(crate) async fn walk_files(dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut entries = match tokio::fs::read_dir(&current).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(path = %current.display(), "local blob directory is absent");
                continue;
            }
            Err(error) => return Err(format!("read dir {}: {error}", current.display())),
        };
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    return Err(format!(
                        "read dir entry under {}: {error}",
                        current.display()
                    ))
                }
            };
            let path = entry.path();
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %path.display(), "local blob vanished during directory walk");
                    continue;
                }
                Err(error) => return Err(format!("stat entry {}: {error}", path.display())),
            };
            if metadata.is_dir() {
                stack.push(path);
            } else if !is_temp_blob_path(&path) {
                files.push((path, mtime_millis(&metadata)?, metadata.len()));
            }
        }
    }
    Ok(files)
}

fn mtime_millis(metadata: &std::fs::Metadata) -> Result<u64, String> {
    Ok(metadata
        .modified()
        .map_err(|e| format!("modified time: {e}"))?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("modified time predates Unix epoch: {e}"))?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    async fn temp_entries(dir: &Path) -> Vec<PathBuf> {
        let mut entries = tokio::fs::read_dir(dir).await.expect("read test directory");
        let mut temps = Vec::new();
        while let Some(entry) = entries.next_entry().await.expect("read test entry") {
            let path = entry.path();
            if is_temp_blob_path(&path) {
                temps.push(path);
            }
        }
        temps
    }

    #[tokio::test]
    async fn direct_file_operations_preserve_bytes_and_report_presence() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let source = tmp.path().join("source").join("blob.bin");
        let copy = tmp.path().join("copy").join("blob.bin");
        let renamed = tmp.path().join("copy").join("renamed.bin");
        let bytes = b"0123456789";

        write_atomic(&source, bytes).await.expect("write source");
        assert!(exists(&source).await.expect("source exists"));
        assert_eq!(file_len(&source).await.expect("source length"), 10);
        assert_eq!(read(&source).await.expect("read source"), bytes);

        write_atomic(&copy, b"old copy").await.expect("seed copy");
        copy_atomic(&source, &copy).await.expect("replace copy");
        assert_eq!(read(&copy).await.expect("read copy"), bytes);

        rename(&copy, &renamed).await.expect("rename copy");
        assert!(!exists(&copy).await.expect("old copy absent"));
        assert_eq!(read(&renamed).await.expect("read renamed"), bytes);
        assert!(remove_file(&renamed).await.expect("remove renamed"));
        assert!(!remove_file(&renamed).await.expect("renamed already absent"));
    }

    #[tokio::test]
    async fn exact_read_keeps_bytes_and_identity_on_one_open_inode() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("blob.bin");
        let original = b"original exact bytes";
        let replacement = b"replacement bytes";
        write_atomic(&path, original).await.expect("write original");
        let mut open = tokio::fs::File::open(&path).await.expect("open original");

        write_atomic(&path, replacement)
            .await
            .expect("replace path after open");
        let (bytes, size, hash) =
            read_open_file_with_facts(&mut open, &path, ExactReadSelection::Whole)
                .await
                .expect("read the already-open exact file");

        assert_eq!(bytes, original);
        assert_eq!(size, original.len() as u64);
        assert_eq!(
            hash,
            crate::protocol::store_commit::ObjectHash::digest(original)
        );
        assert_eq!(read(&path).await.expect("read replacement"), replacement);
    }

    /// An [`OpenFile`] serves every range from the descriptor it opened.
    /// Replacing the path with same-length different bytes — the swap a
    /// per-range re-open by name would silently follow — cannot change what the
    /// handle reads.
    #[tokio::test]
    async fn an_open_file_serves_ranges_from_the_inode_it_opened() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("blob.bin");
        let original = b"original exact bytes";
        let replacement = b"replaced exact bytes";
        assert_eq!(original.len(), replacement.len());
        write_atomic(&path, original).await.expect("write original");

        let open = OpenFile::open(&path).await.expect("open");
        assert_eq!(open.size(), original.len() as u64);

        write_atomic(&path, replacement)
            .await
            .expect("replace the path after the handle opened it");

        assert_eq!(
            open.read_at(9, 5).await.expect("mid-file range"),
            b"exact",
            "the range comes from the opened inode, not the file now at the name",
        );
        assert_eq!(
            open.read_at(0, original.len() as u64)
                .await
                .expect("whole opened file"),
            original,
        );
        assert_eq!(
            open.read_at(0, 8).await.expect("re-read the head"),
            &original[..8],
            "each read positions itself, so an earlier read leaves no cursor behind",
        );
        assert_eq!(
            open.read_at(4, 0).await.expect("zero-length range"),
            Vec::<u8>::new(),
        );
        let past_end = open
            .read_at(original.len() as u64 - 2, 5)
            .await
            .expect_err("a range past the end must fail, not short-read");
        assert!(
            past_end.contains("read 5 bytes at"),
            "the error names the range it could not serve: {past_end}",
        );

        assert_eq!(read(&path).await.expect("read replacement"), replacement);
    }

    #[tokio::test]
    async fn exact_copy_keeps_bytes_and_identity_on_one_open_inode() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let source = tmp.path().join("source.bin");
        let destination = tmp.path().join("destination.bin");
        let original = b"original exact bytes";
        let replacement = b"replacement bytes";
        write_atomic(&source, original)
            .await
            .expect("write original");
        let open = tokio::fs::File::open(&source).await.expect("open original");

        write_atomic(&source, replacement)
            .await
            .expect("replace source path after open");
        let (size, hash) = copy_open_file_atomic_with_facts(open, &source, &destination)
            .await
            .expect("copy the already-open exact file");

        assert_eq!(read(&destination).await.expect("read copy"), original);
        assert_eq!(size, original.len() as u64);
        assert_eq!(
            hash,
            crate::protocol::store_commit::ObjectHash::digest(original)
        );
        assert_eq!(read(&source).await.expect("read replacement"), replacement);
    }

    #[tokio::test]
    async fn staged_file_is_invisible_until_verified_commit() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        write_atomic(&destination, b"prior")
            .await
            .expect("seed destination");
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("allocate staging path");
        staged
            .write_bytes(b"verified")
            .await
            .expect("write staged file");

        assert_eq!(read(&destination).await.unwrap(), b"prior");
        staged.commit().await.expect("commit staged file");
        assert_eq!(read(&destination).await.unwrap(), b"verified");
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn plaintext_stream_fills_the_reserved_stage_before_commit() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let source_path = tmp.path().join("source.bin");
        let destination = tmp.path().join("blob.bin");
        let bytes = b"verified plaintext";
        write_atomic(&source_path, bytes)
            .await
            .expect("write plaintext source");
        let mut source = FilePlaintextReader {
            file: tokio::fs::File::open(&source_path)
                .await
                .expect("open plaintext source"),
            path: source_path,
        };
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("reserve staging file");

        let written = staged
            .write_plaintext(&mut source)
            .await
            .expect("fill reserved staging file");
        assert_eq!(written, bytes.len() as u64);
        assert!(!destination.exists());

        staged.commit().await.expect("publish verified stage");
        assert_eq!(read(&destination).await.expect("read destination"), bytes);
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn byte_stream_fills_the_reserved_stage_before_commit() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        let stream = futures_util::stream::iter([
            Ok::<_, &'static str>(Bytes::from_static(b"verified ")),
            Ok(Bytes::from_static(b"bytes")),
        ]);
        let staged = AtomicStagedFile::create(&destination)
            .await
            .expect("reserve staging file");

        let (staged, written) = staged
            .write_byte_stream(Box::pin(stream))
            .await
            .expect("fill reserved staging file");
        assert_eq!(written, b"verified bytes".len() as u64);
        assert!(!destination.exists());

        staged.commit().await.expect("publish verified stage");
        assert_eq!(
            read(&destination).await.expect("read destination"),
            b"verified bytes"
        );
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn staged_file_publish_rolls_back_when_directory_sync_fails() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("allocate staging path");
        staged
            .write_bytes(b"verified")
            .await
            .expect("write staged file");
        let staged_path = staged.path().to_path_buf();

        let error = commit_staged_file(&staged_path, &destination, |_| async {
            Err("injected directory sync failure".to_string())
        })
        .await
        .expect_err("directory sync failure must reject publication");

        assert_eq!(error, "injected directory sync failure");
        assert!(!destination.exists());
        assert_eq!(
            read(&staged_path).await.expect("read restored stage"),
            b"verified"
        );
    }

    #[tokio::test]
    async fn staged_new_file_refuses_to_replace_an_existing_user_file() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        write_atomic(&destination, b"user file")
            .await
            .expect("seed user destination");
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("allocate staging path");
        staged
            .write_bytes(b"downloaded")
            .await
            .expect("write verified staged file");

        assert_eq!(
            staged.commit_new().await,
            Err(CommitNewFileError::DestinationExists(destination.clone()))
        );
        assert_eq!(read(&destination).await.unwrap(), b"user file");
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn staged_new_file_publishes_complete_verified_bytes() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("allocate staging path");
        staged
            .write_bytes(b"downloaded")
            .await
            .expect("write verified staged file");

        staged.commit_new().await.expect("publish new user file");

        assert_eq!(read(&destination).await.unwrap(), b"downloaded");
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn staged_new_file_rolls_back_when_final_directory_sync_fails() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("blob.bin");
        let mut staged = AtomicStagedFile::create(&destination)
            .await
            .expect("allocate staging path");
        staged
            .write_bytes(b"downloaded")
            .await
            .expect("write verified staged file");
        let staged_path = staged.path().to_path_buf();
        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_count_for_call = sync_count.clone();

        let error = staged
            .commit_new_with_sync(move |_| {
                let invocation = sync_count_for_call.fetch_add(1, Ordering::SeqCst);
                async move {
                    if invocation == 1 {
                        Err("injected final directory sync failure".to_string())
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .expect_err("final directory sync failure must be reported");

        assert_eq!(
            error,
            CommitNewFileError::Filesystem("injected final directory sync failure".to_string())
        );
        assert_eq!(sync_count.load(Ordering::SeqCst), 2);
        assert!(!destination.exists());
        assert!(!staged_path.exists());
    }

    #[tokio::test]
    async fn byte_stream_failure_preserves_destination_and_removes_temp() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("streamed.bin");
        write_atomic(&path, b"committed")
            .await
            .expect("seed destination");
        let stream =
            futures_util::stream::iter([Ok(Bytes::from_static(b"partial")), Err("source failed")]);

        let error = write_byte_stream_atomic(&path, Box::pin(stream))
            .await
            .expect_err("source failure");

        assert!(matches!(
            error,
            ByteStreamWriteError::Source("source failed")
        ));
        assert_eq!(read(&path).await.expect("read destination"), b"committed");
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn canceled_byte_stream_preserves_destination_and_removes_temp() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("streamed.bin");
        write_atomic(&path, b"committed")
            .await
            .expect("seed destination");
        let first_yielded = Arc::new(tokio::sync::Notify::new());
        let first_yielded_for_stream = first_yielded.clone();
        let stream = futures_util::stream::once(async move {
            first_yielded_for_stream.notify_one();
            Ok::<Bytes, &'static str>(Bytes::from_static(b"partial"))
        })
        .chain(futures_util::stream::pending());
        let write_path = path.clone();
        let write =
            tokio::spawn(
                async move { write_byte_stream_atomic(&write_path, Box::pin(stream)).await },
            );
        first_yielded.notified().await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let temps = temp_entries(tmp.path()).await;
                if temps.iter().any(|temp| {
                    std::fs::metadata(temp)
                        .is_ok_and(|metadata| metadata.len() == b"partial".len() as u64)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("partial temp file was written");

        write.abort();
        assert!(write.await.expect_err("write task canceled").is_cancelled());

        assert_eq!(read(&path).await.expect("read destination"), b"committed");
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn walk_absent_root_is_empty() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let absent = tmp.path().join("absent");

        assert!(walk_files(&absent)
            .await
            .expect("walk absent root")
            .is_empty());
    }

    #[tokio::test]
    async fn walk_reports_committed_files_and_filters_temps() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path().join("blobs");
        let committed = root.join("nested").join("blob.bin");
        let temp = root
            .join("nested")
            .join(format!("{TEMP_BLOB_PREFIX}in-progress"));
        write_atomic(&committed, b"blob").await.expect("write blob");
        tokio::fs::write(&temp, b"partial")
            .await
            .expect("write temp");

        let files = walk_files(&root).await.expect("walk files");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, committed);
        assert_eq!(files[0].2, 4);
    }

    #[tokio::test]
    async fn absent_atomic_temp_cleanup_succeeds() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let temp = AtomicTempFile::create_in(tmp.path()).expect("create atomic temp");
        let path = temp.path.clone();
        tokio::fs::remove_file(path)
            .await
            .expect("remove atomic temp before cleanup");

        temp.cleanup()
            .await
            .expect("an already-absent atomic temp is clean");
    }

    #[tokio::test]
    async fn failed_atomic_temp_cleanup_reports_the_remaining_target() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let temp = AtomicTempFile::create_in(tmp.path()).expect("create atomic temp");
        let path = temp.path.clone();
        tokio::fs::remove_file(&path)
            .await
            .expect("remove atomic temp");
        tokio::fs::create_dir(&path)
            .await
            .expect("create cleanup obstruction");

        let error = temp
            .cleanup()
            .await
            .expect_err("an unremovable atomic temp must fail cleanup");

        assert!(
            error.contains("remove temporary blob") && path.exists(),
            "{error}"
        );
        tokio::fs::remove_dir(path)
            .await
            .expect("remove cleanup obstruction");
    }

    #[tokio::test]
    async fn write_atomic_durable_leaves_a_readable_file() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("nested").join("upload_staging.bin");
        let bytes = b"packed outgoing changeset".to_vec();

        write_atomic_durable(&path, &bytes)
            .await
            .expect("durable write");

        assert_eq!(read(&path).await.expect("read back"), bytes);
    }
}
