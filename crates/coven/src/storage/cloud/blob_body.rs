use super::*;

/// Reports how many bytes of a `write` have reached the backend so far.
/// Called with the cumulative byte count as the body uploads; backends that
/// can't observe sub-call progress call it once at the end with the full size.
/// The count is of the bytes handed to `write` (the encrypted payload).
pub type UploadProgress<'a> = dyn Fn(u64) + Send + Sync + 'a;

/// Chunk size the in-memory test backend uses to drive its `UploadProgress`
/// callback in several ticks. Real providers whose resumable API mandates a
/// specific alignment (OneDrive 320 KiB multiples, Google Drive 256 KiB
/// multiples, S3 5 MiB minimum parts) define their own constant.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) const PROGRESS_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// A progress sink that discards its reports. For `write` calls whose payload
/// is a small control file (head pointers, the snapshot) where no per-file
/// progress bar is driven — only the blob outbox surfaces progress.
pub(crate) fn no_progress() -> impl Fn(u64) + Send + Sync {
    |_| {}
}

/// A blob as a **sized stream of already-final bytes**: sealed chunks for an
/// encrypted home, plaintext for a browsable one. Encryption-agnostic and
/// concrete (no `dyn Stream`). [`next_part`](BlobBody::next_part) hands the bytes to a streaming
/// upload in bounded windows so a large blob is never held whole in memory; the
/// only [`collect`](BlobBody::collect) is the single-request path for blobs at or
/// below a provider's multipart threshold.
///
/// Built by the cipher layer (`CloudCipher::open_body`), which knows scope→key and
/// plaintext-vs-encrypted, or by [`from_bytes`](BlobBody::from_bytes) for an
/// in-memory control object / the test backend.
pub struct BlobBody {
    /// Total bytes this body will yield: the encrypted length (see
    /// [`crate::encryption::chunked_encrypted_len`]) for a sealed body, or the
    /// plaintext length for a passthrough one.
    len: u64,
    source: BlobSource,
    /// Final bytes produced by the source but not yet handed out by `next_part`.
    carry: BytesMut,
}

/// Where a [`BlobBody`]'s final bytes come from.
enum BlobSource {
    /// Already-final bytes, handed out once. A control object's sealed bytes, a
    /// small in-memory write, or the in-memory test backend's payload.
    Buffered(Bytes),
    /// Plaintext read incrementally from a local file and, for an encrypted home,
    /// sealed one header-sized chunk at a time.
    File {
        reader: PlaintextReader,
        /// Final bytes emitted before the plaintext stream — the key tag and,
        /// for an encrypted home, the sealed-blob header.
        prefix: Bytes,
        /// `Some` seals each chunk under the scope's key; `None` passes the
        /// plaintext through (a browsable home).
        sealer: Option<SealedBlobSealer>,
        /// Whether any plaintext chunk has been sealed — distinguishes a truly
        /// empty file (which still seals one tag-only chunk, so opening it
        /// authenticates its emptiness) from one that produced chunks and then
        /// drained.
        sealed_any: bool,
        eof: bool,
    },
}

impl BlobSource {
    /// The next run of final bytes, or `None` once the source is exhausted.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, CloudHomeError> {
        match self {
            BlobSource::Buffered(b) => {
                if b.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(b)))
                }
            }
            BlobSource::File {
                reader,
                prefix,
                sealer,
                sealed_any,
                eof,
            } => {
                if !prefix.is_empty() {
                    return Ok(Some(std::mem::take(prefix)));
                }
                if *eof {
                    return Ok(None);
                }
                // Each read is exactly one chunk of the size this blob's header
                // declares, so the sealer's framing and the reader's stride are
                // the same number by construction.
                let stride = match sealer {
                    Some(s) => s.header().chunk_size().get() as usize,
                    None => DEFAULT_BLOB_CHUNK_SIZE.get() as usize,
                };
                let chunk = reader
                    .next_chunk(stride)
                    .await
                    .map_err(CloudHomeError::Transport)?;
                if chunk.is_empty() {
                    *eof = true;
                    // A sealed empty file still emits one tag-only chunk, so a
                    // reader authenticates its emptiness; a plaintext file (or a
                    // sealed one that already produced chunks) ends here.
                    if let Some(s) = sealer {
                        if !*sealed_any {
                            *sealed_any = true;
                            return Ok(Some(Bytes::from(s.seal_chunk(&[]))));
                        }
                    }
                    return Ok(None);
                }
                match sealer {
                    Some(s) => {
                        *sealed_any = true;
                        Ok(Some(Bytes::from(s.seal_chunk(&chunk))))
                    }
                    None => Ok(Some(Bytes::from(chunk))),
                }
            }
        }
    }
}

impl BlobBody {
    /// A body over already-final in-memory bytes — a sealed control object, or a
    /// test payload. `len` is the byte count.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        BlobBody {
            len: data.len() as u64,
            source: BlobSource::Buffered(Bytes::from(data)),
            carry: BytesMut::new(),
        }
    }

    pub async fn from_file(path: &Path) -> Result<Self, String> {
        let len = coven_foundation::local_file::file_len(path).await?;
        let reader = crate::storage::local_file::open_reader(path).await?;
        Ok(Self::from_file_with_prefix(len, reader, None, Vec::new()))
    }

    pub(crate) fn from_file_with_prefix(
        len: u64,
        reader: PlaintextReader,
        sealer: Option<SealedBlobSealer>,
        prefix: Vec<u8>,
    ) -> Self {
        BlobBody {
            len,
            source: BlobSource::File {
                reader,
                prefix: Bytes::from(prefix),
                sealer,
                sealed_any: false,
                eof: false,
            },
            carry: BytesMut::new(),
        }
    }

    /// Total bytes this body yields (encrypted or plaintext length).
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the body yields no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Pull bytes from the source until `carry` holds at least `min` or the source
    /// is exhausted.
    async fn fill(&mut self, min: usize) -> Result<(), CloudHomeError> {
        while self.carry.len() < min {
            match self.source.next_chunk().await? {
                Some(b) => self.carry.extend_from_slice(&b),
                None => break,
            }
        }
        Ok(())
    }

    /// Return at least `min` bytes — exactly `min` when more remain, the remainder
    /// at EOF — or `None` once fully drained. The driver calls this with the
    /// provider's part size, so every part except the last is exactly that size.
    pub async fn next_part(&mut self, min: usize) -> Result<Option<Bytes>, CloudHomeError> {
        let min = min.max(1);
        self.fill(min).await?;
        if self.carry.is_empty() {
            return Ok(None);
        }
        let take = self.carry.len().min(min);
        Ok(Some(self.carry.split_to(take).freeze()))
    }

    /// Drain the whole body into one `Vec`. Used ONLY by the single-request upload
    /// path for blobs at or below a provider's multipart threshold (bounded small).
    pub async fn collect(mut self) -> Result<Vec<u8>, CloudHomeError> {
        let mut out = Vec::with_capacity(self.len as usize);
        out.extend_from_slice(&self.carry);
        self.carry.clear();
        while let Some(b) = self.source.next_chunk().await? {
            out.extend_from_slice(&b);
        }
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn from_test_reader(len: u64, reader: PlaintextReader) -> Self {
        Self::from_file_with_prefix(len, reader, None, Vec::new())
    }
}

/// The one per-provider streaming-upload surface: a session that accepts ordered
/// parts and commits. The central `write_blob` driver opens one of these for a
/// large blob and pumps [`BlobBody`] parts into it — no backend writes its own
/// upload loop, collect, or progress call.
#[async_trait]
pub trait PartSink: Send {
    /// Bytes per part. Every part except the last is exactly this; the last is the
    /// remainder. Encodes each provider's required part size (S3 ≥ 5 MiB, OneDrive
    /// 320 KiB multiples, Drive 256 KiB multiples, ...).
    fn part_size(&self) -> usize;

    /// Send one part. `offset` is its byte offset in the blob; `is_last` marks the
    /// final part (providers that commit on the last call use it).
    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError>;

    /// Cancel the open upload and remove its unpublished provider state. The
    /// upload owner awaits this operation and returns any cleanup failure to its
    /// caller; `Drop` must never block or terminate the process.
    async fn abort(&mut self) -> Result<(), CloudHomeError>;

    /// Commit the upload (e.g. S3 `complete_multipart_upload`); a no-op where the
    /// last `send_part` already committed.
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError>;
}

/// A boxed [`PartSink`] borrowing its home for `'a`.
pub type BoxPartSink<'a> = Box<dyn PartSink + 'a>;

/// Report an operation's failure, folding in a second failure that happened
/// while cleaning up after it. Both are kept: the cleanup failure is what left
/// remote state behind, the operation failure is why cleanup ran at all.
pub(crate) fn combine_cleanup_failure(
    operation: CloudHomeError,
    cleanup: Result<(), CloudHomeError>,
) -> CloudHomeError {
    match cleanup {
        Ok(()) => operation,
        Err(cleanup) => CloudHomeError::CleanupFailed {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        },
    }
}

/// One open multipart upload, including its source body, provider session, and
/// progress reporting. The operation either finishes the provider session or
/// awaits its abort and preserves both the operation and cleanup failures.
pub(crate) struct MultipartUpload<'sink, 'progress> {
    key: String,
    body: BlobBody,
    sink: BoxPartSink<'sink>,
    progress: &'progress UploadProgress<'progress>,
}

impl<'sink, 'progress> MultipartUpload<'sink, 'progress> {
    pub(crate) fn new(
        key: &str,
        body: BlobBody,
        sink: BoxPartSink<'sink>,
        progress: &'progress UploadProgress<'progress>,
    ) -> Self {
        Self {
            key: key.to_string(),
            body,
            sink,
            progress,
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), CloudHomeError> {
        let part_size = self.sink.part_size();
        let total = self.body.len();
        let mut offset = 0u64;
        loop {
            let part = match self.body.next_part(part_size).await {
                Ok(Some(part)) => part,
                Ok(None) if offset == total => break,
                Ok(None) => {
                    let operation = CloudHomeError::Transport(format!(
                        "upload body for {} ended after {offset} of {total} bytes",
                        self.key
                    ));
                    return Err(self.abort(operation).await);
                }
                Err(operation) => return Err(self.abort(operation).await),
            };
            let n = part.len() as u64;
            let is_last = offset + n >= total;
            if let Err(operation) = self.sink.send_part(part, offset, is_last).await {
                return Err(self.abort(operation).await);
            }
            offset += n;
            (self.progress)(offset);
        }
        self.sink.finish().await
    }

    pub(crate) async fn abort(&mut self, operation: CloudHomeError) -> CloudHomeError {
        if matches!(&operation, CloudHomeError::CleanupFailed { .. }) {
            return operation;
        }
        combine_cleanup_failure(operation, self.sink.abort().await)
    }
}
