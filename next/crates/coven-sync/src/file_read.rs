//! Authenticated, bounded ranged reads with cached ciphertext and read-ahead.

use crate::{
    files::{Files, FilesInner},
    FileReadError,
};
use coven_crypto::{ContentHasher, FileKey};
use coven_database::{DbError, FileLocation, FileRef, LocalFileStream};
use coven_format::file::{FileHeader, FILE_HEADER_LEN};
use coven_foundation::files::{FileArea, FileName, StoreReadLock};
use coven_storage::{ByteRange, ObjectPath, StorageFailure};
use std::sync::Arc;

const REQUEST_BYTES: u64 = 1024 * 1024;
/// An open, checked file reference; subsequent row changes cannot redirect it.
/// Its shared store lock prevents deletion until the stream and its I/O finish,
/// including reads still running after the store closes or the caller cancels.
pub struct FileStream {
    id: String,
    source: OpenFile,
    state: tokio::sync::Mutex<ReadState>,
}
enum OpenFile {
    Local(LocalFileStream),
    Uploaded(Arc<UploadedFile>),
}
struct ReadState {
    end: Option<u64>,
    hash: Option<ContentHasher>,
    ahead: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for ReadState {
    fn drop(&mut self) {
        if let Some(task) = &self.ahead {
            task.abort();
        }
    }
}
pub(crate) struct UploadedFile {
    owner: Arc<FilesInner>,
    file: FileRef,
    header: FileHeader,
    key: FileKey,
    path: ObjectPath,
    downloaded: std::sync::atomic::AtomicU64,
    _lock: StoreReadLock,
}
enum ChunkDestination {
    Cache,
    WholeFile,
}
impl Files {
    /// Validate against the current row, then open its authoritative location.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.inner.check_open()?;
        self.inner.database.validate(file).await?;
        let source = match file.location() {
            FileLocation::OnDevice(_) => {
                OpenFile::Local(self.inner.database.open_local(file).await?)
            }
            FileLocation::Uploaded => OpenFile::Uploaded(Arc::new(
                UploadedFile::open(self.inner.clone(), file.clone()).await?,
            )),
        };
        Ok(FileStream {
            id: file.id(),
            source,
            state: tokio::sync::Mutex::new(ReadState {
                end: Some(0),
                hash: Some(ContentHasher::new()),
                ahead: None,
            }),
        })
    }
    /// Read and check the whole file against its row's content hash.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        self.open_file_stream(file)
            .await?
            .read_at(0, file.plaintext_size())
            .await
    }
    /// Fetch an uploaded file through the cache, or check its declared local copy.
    pub async fn ensure_file_on_device(&self, file: &FileRef) -> Result<(), FileReadError> {
        let stream = self.open_file_stream(file).await?;
        let mut range = stream.read_range(0, stream.plaintext_size())?;
        while range.next().await?.is_some() {}
        Ok(())
    }
}
impl FileStream {
    /// Complete plaintext size.
    pub fn plaintext_size(&self) -> u64 {
        match &self.source {
            OpenFile::Local(file) => file.plaintext_size(),
            OpenFile::Uploaded(file) => file.header.size(),
        }
    }
    /// Read exactly the requested range. Network requests cover only missing
    /// chunks, batching adjacent chunks within 1 MiB. Use `read_range` to keep
    /// returned plaintext bounded independently of the requested range length.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError> {
        match &self.source {
            OpenFile::Local(file) => Ok(file.read_at(offset, len).await?),
            OpenFile::Uploaded(file) => {
                file.check_range(offset, len)?;
                let mut state = self.state.lock().await;
                let sequential = state.end == Some(offset);
                if !sequential {
                    if let Some(task) = state.ahead.take() {
                        task.abort();
                    }
                }
                if offset == 0 {
                    state.hash = Some(ContentHasher::new());
                } else if !sequential {
                    state.hash = None;
                }
                let bytes = file.range(offset, len).await?;
                if let Some(hash) = &mut state.hash {
                    hash.update(&bytes);
                }
                if offset + len == file.header.size() {
                    if let Some(hash) = state.hash.take() {
                        if hash.finish() != file.file.content_hash() {
                            return Err(file.integrity());
                        }
                    }
                }
                state.end = Some(offset + len);
                if sequential && offset > 0 && offset + len < file.header.size() {
                    if let Some(task) = state.ahead.take() {
                        task.abort();
                    }
                    let ahead = file.clone();
                    let start = offset + len;
                    state.ahead = Some(tokio::spawn(async move {
                        let length = (ahead.header.size() - start).min(REQUEST_BYTES);
                        if let Err(error) = ahead.range(start, length).await {
                            tracing::debug!(?error, "file read-ahead failed");
                        }
                    }));
                }
                Ok(bytes)
            }
        }
    }
    /// Stream a byte range in bounded plaintext buffers, authenticating each
    /// fetched chunk before yielding its bytes. The final buffer checks the
    /// whole-file hash when reading sequentially from zero through the end.
    pub fn read_range(&self, offset: u64, len: u64) -> Result<FileRangeStream<'_>, FileReadError> {
        if offset
            .checked_add(len)
            .is_none_or(|end| end > self.plaintext_size())
        {
            return Err(FileReadError::RangeOutOfBounds {
                id: self.id.clone(),
                offset,
                end: offset.saturating_add(len),
                size: self.plaintext_size(),
            });
        }
        Ok(FileRangeStream {
            file: self,
            offset,
            remaining: len,
            empty: len == 0,
        })
    }
}
/// A borrowed stream of bounded authenticated plaintext buffers.
pub struct FileRangeStream<'a> {
    file: &'a FileStream,
    offset: u64,
    remaining: u64,
    empty: bool,
}
impl FileRangeStream<'_> {
    /// Yield the next buffer, or `None` after the exact requested range.
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, FileReadError> {
        if self.empty {
            self.empty = false;
            self.file.read_at(self.offset, self.remaining).await?;
            self.remaining = 0;
            return Ok(None);
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let length = self.remaining.min(REQUEST_BYTES - 64 * 1024);
        let bytes = self.file.read_at(self.offset, length).await?;
        self.offset += length;
        self.remaining -= length;
        Ok(Some(bytes))
    }
}
impl UploadedFile {
    pub(crate) async fn open(owner: Arc<FilesInner>, file: FileRef) -> Result<Self, FileReadError> {
        let directory = owner.directory.clone();
        let lock = crate::files::join(tokio::task::spawn_blocking(move || {
            directory.lock_read_only()
        }))
        .await?;
        let coven_format::file_reference::UploadedFileReference { device, id, key } =
            file.uploaded()?.ok_or(DbError::DamagedDatabase)?;
        let path = ObjectPath::file(device, id);
        let _guard = owner.cache.lock().await;
        let cached = owner.database.cached(&file, -1).await?;
        let bytes = match &cached {
            Some(bytes) => bytes.clone(),
            None => fetch(&owner, &file, &path, 0, FILE_HEADER_LEN as u64).await?,
        };
        let header =
            FileHeader::decode(&bytes).map_err(|_| FileReadError::Integrity { id: file.id() })?;
        if header.size() != file.plaintext_size() {
            return Err(FileReadError::Integrity { id: file.id() });
        }
        if cached.is_none() {
            cache(&owner, &file, -1, bytes).await?;
            owner.trim(file.namespace()).await;
        }
        drop(_guard);
        Ok(Self {
            owner,
            file,
            header,
            key,
            path,
            downloaded: std::sync::atomic::AtomicU64::new(0),
            _lock: lock,
        })
    }
    pub(crate) async fn download(&self, progress: impl Fn(u64)) -> Result<(), FileReadError> {
        let mut index = 0;
        let mut hash = ContentHasher::new();
        while index < self.header.chunk_count() {
            let (last, chunks) = self
                .chunks(
                    index,
                    self.header.chunk_count() - 1,
                    ChunkDestination::Cache,
                )
                .await?;
            for chunk in chunks {
                hash.update(&chunk.plaintext);
            }
            progress(self.downloaded.load(std::sync::atomic::Ordering::Relaxed));
            index = last + 1;
        }
        if hash.finish() != self.file.content_hash() {
            return Err(self.integrity());
        }
        Ok(())
    }
    pub(crate) async fn keep_whole(&self, progress: impl Fn(u64)) -> Result<(), FileReadError> {
        let name = FileName::new(self.owner.ids.new_id().to_string()).expect("UUID cache name");
        let reservation = self.owner.database.reserve_cache_file(name.clone()).await?;
        let target = self.owner.directory.file(FileArea::Cache, &name);
        let directory = self.owner.directory.clone();
        // Cleanup cannot release the name while creation still runs after a
        // cancelled await; the blocking task retains its reservation.
        let (reservation, writer) = crate::files::join(tokio::task::spawn_blocking(move || {
            let writer = (|| -> Result<_, FileReadError> {
                let lock = directory.lock_read_only()?;
                Ok(target.create_writer(lock)?)
            })();
            (reservation, writer)
        }))
        .await;
        let mut writer = writer?;
        writer.append(&self.header.encode()).await?;
        let mut hash = ContentHasher::new();
        let mut index = 0;
        while index < self.header.chunk_count() {
            let (last, chunks) = self
                .chunks(
                    index,
                    self.header.chunk_count() - 1,
                    ChunkDestination::WholeFile,
                )
                .await?;
            for chunk in chunks {
                hash.update(&chunk.plaintext);
                writer.append(&chunk.ciphertext).await?;
            }
            progress(self.downloaded.load(std::sync::atomic::Ordering::Relaxed));
            index = last + 1;
        }
        if hash.finish() != self.file.content_hash() {
            return Err(self.integrity());
        }
        writer.finish().await?;
        let _cache = self.owner.cache.lock().await;
        reservation.publish(&self.file).await?;
        Ok(())
    }
    fn integrity(&self) -> FileReadError {
        FileReadError::Integrity { id: self.file.id() }
    }
    fn check_range(&self, offset: u64, len: u64) -> Result<(), FileReadError> {
        if offset
            .checked_add(len)
            .is_none_or(|end| end > self.header.size())
        {
            Err(FileReadError::RangeOutOfBounds {
                id: self.file.id(),
                offset,
                end: offset.saturating_add(len),
                size: self.header.size(),
            })
        } else {
            Ok(())
        }
    }
    pub(crate) async fn range(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError> {
        self.owner.check_open()?;
        self.check_range(offset, len)?;
        let length = usize::try_from(len).map_err(|_| DbError::TooLarge {
            field: "file range",
            actual: len,
            maximum: usize::MAX as u64,
        })?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(length)
            .map_err(|_| DbError::TooLarge {
                field: "file range",
                actual: len,
                maximum: usize::MAX as u64,
            })?;
        if len == 0 {
            return Ok(result);
        }
        let size = u64::from(self.header.chunk_size());
        let mut index = offset / size;
        let last = (offset + len - 1) / size;
        while index <= last {
            let (end, chunks) = self.chunks(index, last, ChunkDestination::Cache).await?;
            for (number, chunk) in chunks.into_iter().enumerate() {
                let plain = chunk.plaintext;
                let position = (index + number as u64) * size;
                let start = offset.saturating_sub(position) as usize;
                let end = (offset + len - position).min(plain.len() as u64) as usize;
                result.extend_from_slice(&plain[start..end]);
            }
            index = end + 1;
        }
        Ok(result)
    }
    // The cache lock protects fetch/publication from eviction and coalesces
    // concurrent readers of the same missing chunks without duplicate requests.
    async fn chunks(
        &self,
        first: u64,
        last: u64,
        destination: ChunkDestination,
    ) -> Result<(u64, Vec<OpenedChunk>), FileReadError> {
        self.owner.check_open()?;
        let _guard = self.owner.cache.lock().await;
        if let Some(bytes) = self.owner.database.cached(&self.file, first as i64).await? {
            let plain = self
                .header
                .open_chunk(&self.key, self.path.as_str(), first, &bytes)
                .map_err(|_| self.integrity())?;
            return Ok((
                first,
                vec![OpenedChunk {
                    plaintext: plain,
                    ciphertext: bytes,
                }],
            ));
        }
        let start = self
            .header
            .chunk(first)
            .map_err(|_| self.integrity())?
            .offset;
        let mut end_index = first;
        let mut end = start
            + (self
                .header
                .chunk(first)
                .map_err(|_| self.integrity())?
                .plaintext_length
                + 16) as u64;
        while end_index < last {
            let next = self
                .header
                .chunk(end_index + 1)
                .map_err(|_| self.integrity())?;
            let next_end = next.offset + (next.plaintext_length + 16) as u64;
            if next_end - start > REQUEST_BYTES {
                break;
            }
            if self
                .owner
                .database
                .cached(&self.file, (end_index + 1) as i64)
                .await?
                .is_some()
            {
                break;
            }
            end_index += 1;
            end = next_end;
        }
        // D12 permits chunks larger than the request cap. Accumulate that one
        // ciphertext chunk from capped requests, then authenticate before use.
        let mut bytes = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let next = (cursor + REQUEST_BYTES).min(end);
            bytes.extend(fetch(&self.owner, &self.file, &self.path, cursor, next).await?);
            cursor = next;
        }
        let mut plain = Vec::new();
        let mut cursor = 0;
        for index in first..=end_index {
            let length = self
                .header
                .chunk(index)
                .map_err(|_| self.integrity())?
                .plaintext_length
                + 16;
            let sealed = bytes
                .get(cursor..cursor + length)
                .ok_or_else(|| self.integrity())?;
            let opened = self
                .header
                .open_chunk(&self.key, self.path.as_str(), index, sealed)
                .map_err(|_| self.integrity())?;
            if matches!(destination, ChunkDestination::Cache) {
                cache(&self.owner, &self.file, index as i64, sealed.to_vec()).await?;
            }
            self.downloaded
                .fetch_add(opened.len() as u64, std::sync::atomic::Ordering::Relaxed);
            plain.push(OpenedChunk {
                plaintext: opened,
                ciphertext: sealed.to_vec(),
            });
            cursor += length;
        }
        // A pin writes these bytes directly to its reserved whole file. Adding
        // them to the chunk cache too could evict a cached tail before pinning
        // reaches it, causing unnecessary downloads and incorrect progress.
        if matches!(destination, ChunkDestination::Cache) {
            self.owner.trim(self.file.namespace()).await;
        }
        Ok((end_index, plain))
    }
}
struct OpenedChunk {
    plaintext: Vec<u8>,
    ciphertext: Vec<u8>,
}

async fn cache(
    owner: &FilesInner,
    file: &FileRef,
    index: i64,
    bytes: Vec<u8>,
) -> Result<(), FileReadError> {
    let name = FileName::new(owner.ids.new_id().to_string()).expect("UUID cache name");
    owner.database.cache_chunk(file, index, name, bytes).await?;
    Ok(())
}
async fn fetch(
    owner: &FilesInner,
    file: &FileRef,
    path: &ObjectPath,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, FileReadError> {
    let storage = owner
        .storage
        .read()
        .expect("storage lock poisoned")
        .clone()
        .ok_or(FileReadError::NoStorage)?;
    let bytes = storage
        .read_range(path, ByteRange::new(start, end)?)
        .await
        .map_err(|error| {
            if error.failure() == StorageFailure::Network {
                FileReadError::Offline { id: file.id() }
            } else {
                FileReadError::Storage(error)
            }
        })?;
    if bytes.len() as u64 != end - start {
        return Err(FileReadError::Integrity { id: file.id() });
    }
    Ok(bytes)
}
