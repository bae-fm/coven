//! Authenticated, bounded ranged reads with cached ciphertext and read-ahead.

use crate::{
    files::file_failure::stored,
    files::file_status::Uploading,
    files::{Files, FilesInner},
    FileReadError,
};
use coven_crypto::{ContentHash, ContentHasher, FileKey};
use coven_database::FileLocationKey;
use coven_database::{DbError, FileLocation, FilePlace, FileRef, LocalFileStream, TransferMeter};
use coven_format::{
    chunks::CHUNK_SIZE,
    file::{FileHeader, FILE_HEADER_LEN},
};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{FileName, StoreReadLock};
use coven_storage::{ByteProgress, ByteRange, ObjectPath};
use std::sync::Arc;

use super::file_error::TransferError;

const REQUEST_BYTES: u64 = 1024 * 1024;
/// An open file. A file never changes its bytes, so nothing later changes
/// what it reads. Its shared store lock prevents deletion until the stream
/// and its I/O finish, including reads still running after the store closes
/// or the caller cancels.
pub struct FileStream {
    id: String,
    source: OpenFile,
    state: tokio::sync::Mutex<ReadState>,
}
enum OpenFile {
    Local {
        stream: LocalFileStream,
        owner: Arc<FilesInner>,
        file: FileRef,
        /// Its upload, while one is queued: reads settle or record its
        /// source's failure there, or on its move. A file with no upload
        /// records nothing; its reads' errors go to the caller.
        uploading: Option<Arc<Uploading>>,
    },
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
    /// Where its bytes are stored.
    location: FileLocation,
    /// The SHA-256 of its bytes, which a whole read checks.
    hash: ContentHash,
    /// The layout the file's size fixes; the stored header must equal it
    /// before any of its chunks is trusted.
    header: FileHeader,
    /// Whether this open has checked the stored header, from the cache or
    /// from storage. Set under the file's fetch lock.
    header_checked: std::sync::atomic::AtomicBool,
    key: FileKey,
    path: ObjectPath,
    _lock: StoreReadLock,
}
enum ChunkDestination<'a> {
    Cache,
    /// A pin's whole file, whose download `meter` hears as its bytes arrive.
    WholeFile(&'a TransferMeter),
}
impl Files {
    /// Open the file as its record now stands: its bytes here while it is on
    /// this device only, uploading or not, or else its cached or stored
    /// bytes. A file only on another device can't be read here (§17.7).
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.inner.check_open()?;
        let mut file = self.inner.current(file).await?;
        if let FilePlace::DeviceOnly { .. } = file.place() {
            let uploading = self.inner.local(&file).await?.map(Arc::new);
            let local = self
                .inner
                .database
                .open_local(&file)
                .await
                .map_err(FileReadError::from);
            match local {
                Ok(Some(stream)) => {
                    return Ok(FileStream::new(
                        &file,
                        OpenFile::Local {
                            stream,
                            owner: self.inner.clone(),
                            file: file.clone(),
                            uploading,
                        },
                    ))
                }
                // Its upload completed meanwhile, and its bytes are stored, or
                // it is only on another device.
                Ok(None) => file = self.inner.current(&file).await?,
                Err(error) => {
                    return match &uploading {
                        Some(uploading) => {
                            self.inner
                                .complete_local_read(file.id(), uploading, Err(error))
                                .await
                        }
                        None => Err(error),
                    }
                }
            }
        }
        if let FilePlace::DeviceOnly { device } = file.place() {
            return Err(self.inner.unreadable(&file, device).await);
        }
        let opened = match UploadedFile::open(self.inner.clone(), file.clone()).await {
            Ok(opened) => opened,
            Err(error) => {
                return self
                    .inner
                    .complete_read(&file, Err(error.into()), false)
                    .await
            }
        };
        Ok(FileStream::new(&file, OpenFile::Uploaded(Arc::new(opened))))
    }
    /// Read the whole file. A stored file is checked against its content
    /// hash; a file this device uploads reads as it is on disk (§17.7).
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        self.open_file_stream(file)
            .await?
            .read_at(0, file.size())
            .await
    }
}
impl FileStream {
    fn new(file: &FileRef, source: OpenFile) -> Self {
        Self {
            id: file.id().to_string(),
            source,
            state: tokio::sync::Mutex::new(ReadState {
                end: Some(0),
                hash: Some(ContentHasher::new()),
                ahead: None,
            }),
        }
    }

    /// Complete plaintext size.
    pub fn plaintext_size(&self) -> u64 {
        match &self.source {
            OpenFile::Local { stream, .. } => stream.plaintext_size(),
            OpenFile::Uploaded(file) => file.header.size(),
        }
    }
    /// Read exactly the requested range. Network requests cover only missing
    /// chunks, batching adjacent chunks within 1 MiB. Use `read_range` to keep
    /// returned plaintext bounded independently of the requested range length.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError> {
        match &self.source {
            OpenFile::Local {
                stream,
                owner,
                file,
                uploading,
            } => {
                let read = stream
                    .read_at(offset, len)
                    .await
                    .map_err(FileReadError::from);
                match uploading {
                    Some(uploading) => owner.complete_local_read(file.id(), uploading, read).await,
                    None => read,
                }
            }
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
                let read = file.range(offset, len).await;
                let bytes = match read {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return file
                            .owner
                            .complete_read(&file.file, Err(error.into()), false)
                            .await
                    }
                };
                let mut whole = false;
                if let Some(hash) = &mut state.hash {
                    hash.update(&bytes);
                }
                if offset + len == file.header.size() {
                    if let Some(hash) = state.hash.take() {
                        if hash.finish() != file.hash {
                            return file
                                .owner
                                .complete_read(
                                    &file.file,
                                    Err(TransferError::Refused(crate::Refusal::ContentHash)),
                                    false,
                                )
                                .await;
                        }
                        whole = true;
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
                        let result = ahead
                            .range(start, length)
                            .await
                            .map_err(TransferError::from);
                        ahead.owner.finish_read_ahead(&ahead.file, result).await;
                    }));
                }
                file.owner.complete_read(&file.file, Ok(bytes), whole).await
            }
        }
    }
    /// Stream a byte range in bounded plaintext buffers, authenticating each
    /// fetched chunk before yielding its bytes. For a stored file, the final
    /// buffer checks the whole-file hash when reading sequentially from zero
    /// through the end.
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
        let length = self.remaining.min(REQUEST_BYTES - CHUNK_SIZE as u64);
        let bytes = self.file.read_at(self.offset, length).await?;
        self.offset += length;
        self.remaining -= length;
        Ok(Some(bytes))
    }
}
impl UploadedFile {
    /// Open an uploaded file without a request when its header is cached.
    /// Otherwise the first fetch checks the header: one starting at chunk
    /// zero carries it in the same request, and any other first fetch reads
    /// it alone (§17.7). An empty file's header is the whole object, so it
    /// is checked here.
    pub(crate) async fn open(owner: Arc<FilesInner>, file: FileRef) -> Result<Self, FileReadError> {
        let (location, hash) = stored(&file)?;
        let location = location.clone();
        let directory = owner.directory.clone();
        let lock = crate::files::join(tokio::task::spawn_blocking(move || {
            directory.lock_read_only()
        }))
        .await?;
        let coven_format::file_reference::FileReference { device, id, key } =
            location.reference()?;
        let path = ObjectPath::file(owner.directory.id(), device, id);
        let opened = Self {
            header: FileHeader::new(file.size()),
            header_checked: std::sync::atomic::AtomicBool::new(false),
            owner,
            file,
            location,
            hash,
            key,
            path,
            _lock: lock,
        };
        if opened.header.chunk_count() == 0 {
            let _file = opened.owner.lock_file(&opened.file, &opened.location).await;
            let _guard = opened.owner.cache.read().await;
            if !opened.cached_header().await? {
                opened.fetch_header().await?;
            }
        }
        Ok(opened)
    }
    /// Accept the header cached for this file, if one is.
    async fn cached_header(&self) -> Result<bool, FileReadError> {
        if self
            .header_checked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(true);
        }
        let Some(bytes) = self.owner.database.cached(&self.file, -1).await? else {
            return Ok(false);
        };
        self.accept_header(&bytes)?;
        Ok(true)
    }
    /// Read the header alone, check it and cache it.
    async fn fetch_header(&self) -> Result<(), FileReadError> {
        let bytes = fetch(
            &self.owner,
            &self.file,
            &self.path,
            0,
            FILE_HEADER_LEN as u64,
            &ChunkDestination::Cache,
        )
        .await?;
        self.publish_header(bytes).await
    }
    /// Check fetched header bytes, then cache them before any chunk.
    async fn publish_header(&self, bytes: Vec<u8>) -> Result<(), FileReadError> {
        self.accept_header(&bytes)?;
        cache(&self.owner, &self.file, -1, 1, bytes).await?;
        self.owner.trim(self.file.namespace()).await;
        Ok(())
    }
    /// The stored header must be the one the row's size fixes; a newer format
    /// needs an update, anything else is damage at the fixed path.
    fn accept_header(&self, bytes: &[u8]) -> Result<(), FileReadError> {
        let header = FileHeader::decode(bytes).map_err(|error| match error {
            coven_format::Error::UnsupportedVersion(version)
                if version > coven_format::FORMAT_VERSION =>
            {
                FileReadError::UpdateRequired(crate::RequiredUpdate::CovenFormat { version })
            }
            _ => self.integrity(),
        })?;
        if header != self.header {
            return Err(self.integrity());
        }
        self.header_checked
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "a size whose encrypted layout overflows is the disagreement Integrity names"
    )]
    pub(crate) async fn keep_whole(&self) -> Result<(), TransferError> {
        // In flight until it ends, by finishing, failing or being dropped.
        let downloading = self.owner.downloading.start(
            self.location.uploader(),
            self.location.object(),
            vec![self.file.id()],
            coven_database::TransferProgress {
                bytes_transferred: 0,
                bytes_total: self.header.encrypted_size().map_err(|_| self.integrity())?,
            },
        );
        // How far through the encrypted object the whole file has got, in
        // order: cached chunks count as they are copied in, fetched ones as
        // their bytes arrive.
        let meter = downloading.meter();
        let (reservation, mut writer) = self.owner.create_cache_file(&self.file).await?;
        let header = self.header.encode();
        writer.append(&header).await?;
        let mut written = header.len() as u64;
        meter.reached(written);
        let mut hash = ContentHasher::new();
        let mut index = 0;
        while index < self.header.chunk_count() {
            let (last, chunks) = self
                .chunks(
                    index,
                    self.header.chunk_count() - 1,
                    ChunkDestination::WholeFile(&meter),
                )
                .await?;
            for chunk in chunks {
                hash.update(&chunk.plaintext);
                writer.append(&chunk.ciphertext).await?;
                written += chunk.ciphertext.len() as u64;
                meter.reached(written);
            }
            index = last + 1;
        }
        if hash.finish() != self.hash {
            return Err(TransferError::Refused(crate::Refusal::ContentHash));
        }
        writer.finish().await?;
        let _cache = self.owner.cache.write().await;
        reservation.publish().await?;
        Ok(())
    }
    fn integrity(&self) -> FileReadError {
        FileReadError::Integrity {
            id: self.file.id().to_string(),
            cause: None,
        }
    }
    fn check_range(&self, offset: u64, len: u64) -> Result<(), FileReadError> {
        if offset
            .checked_add(len)
            .is_none_or(|end| end > self.header.size())
        {
            Err(FileReadError::RangeOutOfBounds {
                id: self.file.id().to_string(),
                offset,
                end: offset.saturating_add(len),
                size: self.header.size(),
            })
        } else {
            Ok(())
        }
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "a size past the target integer is TooLarge, which names the field and size"
    )]
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
        let size = CHUNK_SIZE as u64;
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
    // Reads of one file coalesce; independent files can fetch concurrently.
    // The shared cache guard excludes eviction and store close during a fetch.
    #[expect(
        clippy::map_err_ignore,
        reason = "a chunk the file size's layout lacks is the disagreement Integrity names"
    )]
    async fn chunks(
        &self,
        first: u64,
        last: u64,
        destination: ChunkDestination<'_>,
    ) -> Result<(u64, Vec<OpenedChunk>), FileReadError> {
        self.owner.check_open()?;
        let _file = self.owner.lock_file(&self.file, &self.location).await;
        let _guard = self.owner.cache.read().await;
        let cached = self.owner.database.cached(&self.file, first as i64).await?;
        // An unchecked header comes with an uncached chunk zero; any other
        // first read checks it alone (§17.7).
        let mut with_header = false;
        if !self.cached_header().await? {
            if first == 0 && cached.is_none() {
                with_header = true;
            } else {
                self.fetch_header().await?;
            }
        }
        if let Some(bytes) = cached {
            return Ok((first, vec![self.open_cached(first, bytes)?]));
        }
        let start = self
            .header
            .chunk(first)
            .map_err(|_| self.integrity())?
            .offset;
        let mut end_index = first;
        // The fetch stops at the first cached chunk after the ones it needs.
        // That chunk's bytes, read here, finish this step, so it is read once
        // and counts as read before the trim that follows caching the fetched
        // range.
        let mut stop = None;
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
            let request_start = if with_header { 0 } else { start };
            if next_end - request_start > REQUEST_BYTES {
                break;
            }
            if let Some(bytes) = self
                .owner
                .database
                .cached(&self.file, (end_index + 1) as i64)
                .await?
            {
                stop = Some(bytes);
                break;
            }
            end_index += 1;
            end = next_end;
        }
        let bytes = if with_header {
            // The header precedes chunk zero: one request carries both.
            let mut bytes =
                fetch(&self.owner, &self.file, &self.path, 0, end, &destination).await?;
            let chunks = bytes.split_off(FILE_HEADER_LEN);
            self.publish_header(bytes).await?;
            chunks
        } else {
            fetch(
                &self.owner,
                &self.file,
                &self.path,
                start,
                end,
                &destination,
            )
            .await?
        };
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
                .map_err(|error| FileReadError::chunk(self.file.id().to_string(), error))?;
            plain.push(OpenedChunk {
                plaintext: opened,
                ciphertext: sealed.to_vec(),
            });
            cursor += length;
        }
        // Every chunk of the range authenticated: it is published as one
        // file and one row. A failed chunk publishes none of it. A pin writes
        // these bytes to its reserved whole file instead; adding them to the
        // cache too could evict a cached tail before pinning reaches it.
        if matches!(destination, ChunkDestination::Cache) {
            cache(
                &self.owner,
                &self.file,
                first as i64,
                (end_index - first + 1) as i64,
                bytes,
            )
            .await?;
            self.owner.trim(self.file.namespace()).await;
        }
        if let Some(bytes) = stop {
            end_index += 1;
            plain.push(self.open_cached(end_index, bytes)?);
        }
        Ok((end_index, plain))
    }
    /// Authenticate chunk `index`'s cached bytes.
    fn open_cached(&self, index: u64, bytes: Vec<u8>) -> Result<OpenedChunk, FileReadError> {
        let plaintext = self
            .header
            .open_chunk(&self.key, self.path.as_str(), index, &bytes)
            .map_err(|error| FileReadError::chunk(self.file.id().to_string(), error))?;
        Ok(OpenedChunk {
            plaintext,
            ciphertext: bytes,
        })
    }
}
struct OpenedChunk {
    plaintext: Vec<u8>,
    ciphertext: Vec<u8>,
}

async fn cache(
    owner: &FilesInner,
    file: &FileRef,
    first: i64,
    chunks: i64,
    bytes: Vec<u8>,
) -> Result<(), FileReadError> {
    let name = FileName::new(owner.ids.new_id().to_string()).expect("UUID cache name");
    owner
        .database
        .cache_range(file, first, chunks, name, bytes)
        .await?;
    Ok(())
}
/// Fetch `start..end` of the file's object. For a pin's whole file, its
/// meter hears the furthest through the object the bytes received reach:
/// storage tells it as the stream is read, and CloudKit also as each part's
/// fetch goes, ahead of the read.
async fn fetch(
    owner: &FilesInner,
    file: &FileRef,
    path: &ObjectPath,
    start: u64,
    end: u64,
    destination: &ChunkDestination<'_>,
) -> Result<Vec<u8>, FileReadError> {
    let storage = owner
        .storage
        .read()
        .expect("storage lock poisoned")
        .clone()
        .ok_or(FileReadError::NoStorage)?;
    // A stored file's record names it only once it is stored (§17.2):
    // confirmed absence means storage lost it, never a provider failure or
    // an upload to wait for.
    let received = match destination {
        ChunkDestination::WholeFile(meter) => {
            let meter = (*meter).clone();
            let furthest = std::sync::atomic::AtomicU64::new(start);
            ByteProgress::new(move |reached| {
                let before = furthest.fetch_max(reached, std::sync::atomic::Ordering::Relaxed);
                if reached > before {
                    meter.reached(reached);
                }
            })
        }
        ChunkDestination::Cache => ByteProgress::unread(),
    };
    let mut stream = match storage
        .stream_object_reporting(path, Some(ByteRange::new(start, end)?), &received)
        .await
    {
        Ok(stream) => stream,
        Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => {
            let (location, _) = stored(file)?;
            return Err(owner.absence_error(file, location));
        }
        // Every requested range lies within the file's encrypted size; an
        // object too short to hold it is damage at the fixed path.
        Err(error) if error.failure() == coven_storage::StorageFailure::InvalidRange => {
            return Err(FileReadError::Integrity {
                id: file.id().to_string(),
                cause: None,
            })
        }
        Err(error) => return Err(error.into()),
    };
    // The reply describes the complete object (§20.5). Any size but the file's is
    // damage, as file status says, even when the requested range is there.
    crate::files::file_status::complete(file, stream.metadata())?;
    let mut bytes = Vec::with_capacity((end - start) as usize);
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let length = stream.read(&mut buffer).await?;
        if length == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..length]);
    }
    if bytes.len() as u64 != end - start {
        return Err(FileReadError::Integrity {
            id: file.id().to_string(),
            cause: None,
        });
    }
    Ok(bytes)
}
