use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncReadExt;

/// The filename prefix an atomic blob write gives its in-progress temp sibling
/// (`.tmp.<uuid>`) before the fsync-then-rename that makes it the committed
/// destination.
pub const TEMP_BLOB_PREFIX: &str = ".tmp.";

/// Whether `path`'s file name marks it as an atomic-write temp sibling.
pub fn is_temp_blob_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(TEMP_BLOB_PREFIX))
}

pub struct PlaintextReader(Box<dyn PlaintextChunkReader>);

impl PlaintextReader {
    pub async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        self.0
            .next_chunk(max)
            .await
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlaintextChunkError {
    #[error(transparent)]
    Remote(#[from] crate::sync::storage::StorageError),
    #[error("invalid remote content: {0}")]
    InvalidContent(String),
    #[error("local plaintext source: {0}")]
    Local(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StreamWriteError {
    #[error(transparent)]
    Source(#[from] PlaintextChunkError),
    #[error("local destination: {0}")]
    Local(String),
}

#[derive(Debug)]
pub enum ByteStreamWriteError<E> {
    Source(E),
    Local(String),
}

#[async_trait]
pub trait PlaintextChunkReader: Send {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, PlaintextChunkError>;
}

#[async_trait]
trait AtomicChunkSource {
    type Error: Send;

    async fn next_atomic_chunk(&mut self) -> Result<Option<Bytes>, Self::Error>;
}

struct PlaintextAtomicChunkSource<'a>(&'a mut dyn PlaintextChunkReader);

#[async_trait]
impl AtomicChunkSource for PlaintextAtomicChunkSource<'_> {
    type Error = PlaintextChunkError;

    async fn next_atomic_chunk(&mut self) -> Result<Option<Bytes>, Self::Error> {
        let chunk = self.0.next_chunk(1 << 20).await?;
        Ok((!chunk.is_empty()).then(|| Bytes::from(chunk)))
    }
}

struct ByteStreamAtomicChunkSource<E> {
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>,
}

#[async_trait]
impl<E: Send> AtomicChunkSource for ByteStreamAtomicChunkSource<E> {
    type Error = E;

    async fn next_atomic_chunk(&mut self) -> Result<Option<Bytes>, Self::Error> {
        self.stream.next().await.transpose()
    }
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

impl AtomicTempFile {
    fn create(path: PathBuf) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("create temp blob {}: {error}", path.display()))?;
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
        self.armed = false;
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

pub async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open local blob {} for streaming: {e}", path.display()))?;
    Ok(PlaintextReader(Box::new(FilePlaintextReader {
        file,
        path: path.to_path_buf(),
    })))
}

pub async fn file_len(path: &Path) -> Result<u64, String> {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|e| format!("stat local blob {}: {e}", path.display()))
}

pub async fn copy_atomic(src: &Path, dst: &Path) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let parent = dst
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", dst.display()))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|e| format!("create parent dir for {}: {e}", dst.display()))?;
    let tmp = temp_sibling(parent);

    let copy = async {
        let mut input = tokio::fs::File::open(src)
            .await
            .map_err(|e| format!("open pin source {}: {e}", src.display()))?;
        let mut output = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| format!("create temp pin {}: {e}", tmp.display()))?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let read = input
                .read(&mut buf)
                .await
                .map_err(|e| format!("read pin source {}: {e}", src.display()))?;
            if read == 0 {
                break;
            }
            output
                .write_all(&buf[..read])
                .await
                .map_err(|e| format!("write temp pin {}: {e}", tmp.display()))?;
        }
        output
            .sync_all()
            .await
            .map_err(|e| format!("fsync temp pin {}: {e}", tmp.display()))
    }
    .await;
    if let Err(error) = copy {
        remove_failed_temp(&tmp, "copy").await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&tmp, dst).await {
        remove_failed_temp(&tmp, "rename").await;
        return Err(format!(
            "rename temp pin {} -> {}: {error}",
            tmp.display(),
            dst.display()
        ));
    }
    Ok(())
}

pub async fn write_stream_atomic(
    path: &Path,
    source: &mut dyn PlaintextChunkReader,
) -> Result<u64, StreamWriteError> {
    let mut source = PlaintextAtomicChunkSource(source);
    write_atomic_chunks(path, &mut source)
        .await
        .map_err(|error| match error {
            AtomicChunkWriteError::Source(error) => StreamWriteError::Source(error),
            AtomicChunkWriteError::Local(error) => StreamWriteError::Local(error),
        })
}

pub async fn write_byte_stream_atomic<E: Send>(
    path: &Path,
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>,
) -> Result<u64, ByteStreamWriteError<E>> {
    let mut source = ByteStreamAtomicChunkSource { stream };
    write_atomic_chunks(path, &mut source)
        .await
        .map_err(|error| match error {
            AtomicChunkWriteError::Source(error) => ByteStreamWriteError::Source(error),
            AtomicChunkWriteError::Local(error) => ByteStreamWriteError::Local(error),
        })
}

async fn write_atomic_chunks<S: AtomicChunkSource>(
    path: &Path,
    source: &mut S,
) -> Result<u64, AtomicChunkWriteError<S::Error>> {
    use tokio::io::AsyncWriteExt;

    let parent = path.parent().ok_or_else(|| {
        AtomicChunkWriteError::Local(format!("blob path has no parent dir: {}", path.display()))
    })?;
    tokio::fs::create_dir_all(parent).await.map_err(|e| {
        AtomicChunkWriteError::Local(format!("create parent dir for {}: {e}", path.display()))
    })?;
    let mut temp =
        AtomicTempFile::create(temp_sibling(parent)).map_err(AtomicChunkWriteError::Local)?;
    let mut written = 0u64;
    while let Some(chunk) = source
        .next_atomic_chunk()
        .await
        .map_err(AtomicChunkWriteError::Source)?
    {
        temp.file_mut().write_all(&chunk).await.map_err(|e| {
            AtomicChunkWriteError::Local(format!("write temp blob {}: {e}", temp.path.display()))
        })?;
        written += chunk.len() as u64;
    }
    temp.file_mut().sync_all().await.map_err(|e| {
        AtomicChunkWriteError::Local(format!("fsync temp blob {}: {e}", temp.path.display()))
    })?;
    temp.close();
    std::fs::rename(&temp.path, path).map_err(|error| {
        AtomicChunkWriteError::Local(format!(
            "rename temp blob {} -> {}: {error}",
            temp.path.display(),
            path.display()
        ))
    })?;
    temp.disarm();
    Ok(written)
}

pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
    tokio::fs::read(path)
        .await
        .map_err(|e| format!("read local blob {}: {e}", path.display()))
}

pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    if len == 0 {
        return Ok(Vec::new());
    }
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open local blob {} for ranged read: {e}", path.display()))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| format!("seek local blob {} to {offset}: {e}", path.display()))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf).await.map_err(|e| {
        format!(
            "read {len} bytes at {offset} from local blob {}: {e}",
            path.display()
        )
    })?;
    Ok(buf)
}

pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let parent = path
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
    let tmp = temp_sibling(parent);

    let write = async {
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| format!("create temp blob {}: {e}", tmp.display()))?;
        file.write_all(bytes)
            .await
            .map_err(|e| format!("write temp blob {}: {e}", tmp.display()))?;
        file.sync_all()
            .await
            .map_err(|e| format!("fsync temp blob {}: {e}", tmp.display()))
    }
    .await;
    if let Err(error) = write {
        remove_failed_temp(&tmp, "write").await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&tmp, path).await {
        remove_failed_temp(&tmp, "rename").await;
        return Err(format!(
            "rename temp blob {} -> {}: {error}",
            tmp.display(),
            path.display()
        ));
    }
    Ok(())
}

/// Write atomically and fsync the parent directory containing the committed name.
pub async fn write_atomic_durable(path: &Path, bytes: &[u8]) -> Result<(), String> {
    write_atomic(path, bytes).await?;
    sync_parent_dir(path).await
}

pub async fn exists(path: &Path) -> Result<bool, String> {
    tokio::fs::try_exists(path)
        .await
        .map_err(|e| format!("check local blob {}: {e}", path.display()))
}

pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
    tokio::fs::rename(from, to)
        .await
        .map_err(|e| format!("rename {} -> {}: {e}", from.display(), to.display()))
}

pub async fn remove_file(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove file {}: {error}", path.display())),
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove dir tree {}: {error}", path.display())),
    }
}

pub async fn create_dir_all(path: &Path) -> Result<(), String> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|e| format!("create dir tree {}: {e}", path.display()))
}

pub async fn sync_parent_dir(path: &Path) -> Result<(), String> {
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

/// Every committed file under `dir` as `(path, mtime_ms, size)`.
pub async fn walk_files(dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
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

fn temp_sibling(parent: &Path) -> PathBuf {
    parent.join(format!("{TEMP_BLOB_PREFIX}{}", uuid::Uuid::new_v4()))
}

async fn remove_failed_temp(path: &Path, operation: &str) {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %path.display(), %operation, %error, "could not remove temporary blob");
        }
    }
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
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    struct ChunkSource {
        chunks: VecDeque<Result<Vec<u8>, PlaintextChunkError>>,
    }

    impl ChunkSource {
        fn new(chunks: impl IntoIterator<Item = Result<Vec<u8>, PlaintextChunkError>>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
            }
        }
    }

    #[async_trait]
    impl PlaintextChunkReader for ChunkSource {
        async fn next_chunk(&mut self, _max: usize) -> Result<Vec<u8>, PlaintextChunkError> {
            match self.chunks.pop_front() {
                Some(chunk) => chunk,
                None => Ok(Vec::new()),
            }
        }
    }

    struct WarningSubscriber {
        warnings: Arc<AtomicUsize>,
    }

    impl tracing::Subscriber for WarningSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.warnings.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

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
        assert_eq!(
            read_range(&source, 3, 4).await.expect("read range"),
            b"3456"
        );

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
    async fn streamed_write_commits_all_chunks() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("streamed.bin");
        let mut source = ChunkSource::new([
            Ok(b"first ".to_vec()),
            Ok(b"second".to_vec()),
            Ok(Vec::new()),
        ]);

        let written = write_stream_atomic(&path, &mut source)
            .await
            .expect("stream write");

        assert_eq!(written, 12);
        assert_eq!(
            read(&path).await.expect("read streamed file"),
            b"first second"
        );
        let mut reader = open_reader(&path).await.expect("open streamed file");
        assert_eq!(
            reader.next_chunk(6).await.expect("read first chunk"),
            b"first "
        );
        assert_eq!(
            reader.next_chunk(6).await.expect("read second chunk"),
            b"second"
        );
        assert!(reader
            .next_chunk(6)
            .await
            .expect("read end of streamed file")
            .is_empty());
        assert!(temp_entries(tmp.path()).await.is_empty());
    }

    #[tokio::test]
    async fn streamed_source_failure_preserves_destination_and_removes_temp() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("streamed.bin");
        write_atomic(&path, b"committed")
            .await
            .expect("seed destination");
        let mut source = ChunkSource::new([
            Ok(b"partial".to_vec()),
            Err(PlaintextChunkError::InvalidContent(
                "source failed".to_string(),
            )),
        ]);

        let error = write_stream_atomic(&path, &mut source)
            .await
            .expect_err("source failure");

        assert_eq!(
            error,
            StreamWriteError::Source(PlaintextChunkError::InvalidContent(
                "source failed".to_string(),
            ))
        );
        assert_eq!(read(&path).await.expect("read destination"), b"committed");
        assert!(temp_entries(tmp.path()).await.is_empty());
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
    async fn absent_failed_temp_cleanup_does_not_warn() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let warnings = Arc::new(AtomicUsize::new(0));
        let subscriber = WarningSubscriber {
            warnings: warnings.clone(),
        };
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        remove_failed_temp(&tmp.path().join("absent"), "streamed write").await;

        assert_eq!(warnings.load(Ordering::SeqCst), 0);
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
