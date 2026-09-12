use super::cipher::*;
use super::*;

/// How a cloud home names its blob objects. Paired with the at-rest
/// [`CloudCipher`] by the home's [`HomeStorage`](coven_foundation::config::HomeStorage): an
/// opaque home is `Hashed` + encrypted, a browsable home is `Plain` + plaintext.
#[derive(Clone, Copy)]
pub enum BlobPathScheme {
    /// Locator-addressed `{namespace}/opaque/{locator_hash}` (an opaque home).
    Hashed,
    /// A readable path with an immutable version:
    /// `{namespace}/readable/{cloud_path}/.coven-versions/{locator_hash}`.
    /// A browsable home requires the consumer's `cloud_path` on every blob.
    Plain,
}

impl BlobPathScheme {
    /// The blob-path scheme a home's storage mode selects: an opaque home
    /// obfuscates (`Hashed`), a browsable home is readable (`Plain`).
    pub fn for_storage(storage: coven_foundation::config::HomeStorage) -> Self {
        if storage.is_opaque() {
            BlobPathScheme::Hashed
        } else {
            BlobPathScheme::Plain
        }
    }
}

/// The two numbers that decide what a blob transfer costs. They are independent
/// on purpose: the chunk is fixed when a blob is sealed and bounds how little a
/// read can fetch, so it sets how long a seek waits for its first byte; the
/// window is a live reader-side choice about how much one request carries, so it
/// sets how many round-trips a long read costs. Neither can be derived from the
/// other, and changing the window never touches a stored blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobChunking {
    chunk: std::num::NonZeroU32,
    window: std::num::NonZeroU64,
}

impl BlobChunking {
    /// 64 KiB chunks read one mebibyte of stored bytes at a time.
    pub const DEFAULT: Self = Self {
        chunk: coven_keys::encryption::DEFAULT_BLOB_CHUNK_SIZE,
        window: match std::num::NonZeroU64::new(1 << 20) {
            Some(window) => window,
            None => unreachable!(),
        },
    };

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new(chunk: std::num::NonZeroU32, window: std::num::NonZeroU64) -> Self {
        Self { chunk, window }
    }

    pub fn chunk(self) -> std::num::NonZeroU32 {
        self.chunk
    }

    pub fn window(self) -> std::num::NonZeroU64 {
        self.window
    }
}

/// Serves plaintext ranges of one stored blob by fetching only the sealed chunks
/// that cover them. A read costs the chunks it touches and nothing else — never
/// the whole object, however many ranges the stream asks for.
///
/// Opening a sealed blob reads its `[key tag][header]` prefix once, which is
/// what names the key and the chunk size; every later range is arithmetic over
/// that header plus one ranged request per
/// [window](BlobChunking::window)-worth of chunks. A chunk that opens is
/// authentic — its tag covers its bytes, its index, and the header — so there is
/// nothing else to check and no whole-object pass to amortize.
pub struct BlobRangeReader {
    exact: Arc<dyn ExactCloudHome>,
    slot: coven_protocol::objects::ObjectSlot,
    opener: coven_keys::encryption::SealedBlobOpener,
    plaintext_size: u64,
    window: std::num::NonZeroU64,
}

impl BlobRangeReader {
    pub(crate) fn new(
        exact: Arc<dyn ExactCloudHome>,
        slot: coven_protocol::objects::ObjectSlot,
        opener: coven_keys::encryption::SealedBlobOpener,
        plaintext_size: u64,
        window: std::num::NonZeroU64,
    ) -> Self {
        Self {
            exact,
            slot,
            opener,
            plaintext_size,
            window,
        }
    }

    /// The blob's whole plaintext length, as its row declares it.
    pub fn plaintext_size(&self) -> u64 {
        self.plaintext_size
    }

    /// Read exactly `len` plaintext bytes at `offset`. A range past the blob's
    /// end is an error, never a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            StorageError::Storage(format!("blob range overflow: offset={offset}, len={len}"))
        })?;
        if end > self.plaintext_size {
            return Err(StorageError::Storage(format!(
                "blob range {offset}..{end} exceeds blob size {}",
                self.plaintext_size
            )));
        }
        let header = self.opener.header();
        let chunks =
            header
                .covering_chunks(offset, end)
                .map_err(|source| StorageError::Decryption {
                    context: format!("blob range {offset}..{end}"),
                    source: source.into(),
                })?;
        let mut plaintext = Vec::with_capacity(len as usize);
        for run in header.request_runs(chunks, self.window) {
            let span = header.sealed_span(run.clone());
            let sealed = self
                .read_stored(
                    KeyTag::LEN as u64 + span.start,
                    KeyTag::LEN as u64 + span.end,
                )
                .await?;
            let covered = header.plaintext_span(run.clone());
            let opened = self.opener.open_chunks(run, &sealed).map_err(|error| {
                StorageError::Decryption {
                    context: format!("blob range {offset}..{end}"),
                    source: error.into(),
                }
            })?;
            let from = (offset.max(covered.start) - covered.start) as usize;
            let to = (end.min(covered.end) - covered.start) as usize;
            plaintext.extend_from_slice(&opened[from..to]);
        }
        Ok(plaintext)
    }

    /// One ranged request against the stored object.
    async fn read_stored(&self, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let bytes = self
            .exact
            .read_range_at(&self.slot, start, end)
            .await
            .map_err(StorageError::from)?;
        // A provider that ignored the range and answered with more (or less)
        // than was asked for has not served this range; splicing its answer
        // would silently read the wrong bytes.
        if bytes.len() as u64 != end - start {
            return Err(StorageError::InvalidContent(format!(
                "ranged read of {} returned {} bytes for {start}..{end}",
                self.slot.logical_key(),
                bytes.len()
            )));
        }
        Ok(bytes)
    }
}

/// Where a stored blob's bytes come from: a provider body being received, or a
/// local spool this device wrote and is re-verifying. Either way the reader
/// sees one sequence of stored bytes and ends when the source does.
pub(crate) enum StoredBlobSource {
    Stream {
        stream: crate::cloud::CloudObjectStream,
        /// What a provider buffer carried past the length the reader asked
        /// for. The framing reads exact lengths, so an overlong buffer is held
        /// here rather than spilling into the next chunk.
        pending: bytes::Bytes,
    },
    File(crate::local_file::PlaintextReader),
}

impl StoredBlobSource {
    pub(crate) fn stream(stream: crate::cloud::CloudObjectStream) -> Self {
        Self::Stream {
            stream,
            pending: bytes::Bytes::new(),
        }
    }

    /// Up to `max` more stored bytes. An empty result means the source ended.
    async fn next_bytes(&mut self, max: usize) -> Result<Vec<u8>, StorageError> {
        match self {
            Self::File(reader) => reader.next_chunk(max).await.map_err(StorageError::from),
            Self::Stream { stream, pending } => {
                use futures_util::StreamExt as _;

                while pending.is_empty() {
                    match stream.next().await {
                        None => return Ok(Vec::new()),
                        Some(Err(error)) => return Err(StorageError::from(error)),
                        Some(Ok(bytes)) => *pending = bytes,
                    }
                }
                let taken = pending.split_to(max.min(pending.len()));
                Ok(taken.to_vec())
            }
        }
    }
}

pub(crate) enum ExactBlobOpening {
    Browsable,
    Opaque {
        opener: coven_keys::encryption::SealedBlobOpener,
        next_chunk: u64,
    },
}

/// Opens one stored blob as it arrives and withholds EOF until the source has
/// ended, every chunk of the framing has opened, and the bytes that passed
/// through are the ones the signed reference names.
///
/// Nothing here trusts a length declared inside the body: the header's
/// plaintext length frames the chunks, and completion is the source ending
/// after the last authenticated chunk with nothing trailing it.
pub(crate) struct ExactBlobPlaintextReader {
    source: StoredBlobSource,
    opening: ExactBlobOpening,
    remaining: u64,
    hasher: Option<coven_protocol::blob::ContentHasher>,
    expected_hash: ObjectHash,
    locator_hash: ObjectHash,
    pending: Vec<u8>,
    pending_offset: usize,
    /// Every byte taken from the source, counted and hashed, so the stored
    /// object's identity is checked against the reference without a second
    /// pass over a file.
    stored_size: u64,
    stored_hasher: sha2::Sha256,
    expected_stored_size: u64,
    expected_stored_hash: ObjectHash,
    ended: bool,
}

impl ExactBlobPlaintextReader {
    pub(crate) async fn new(
        mut source: StoredBlobSource,
        store_id: &str,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
    ) -> Result<Self, StorageError> {
        use sha2::Digest as _;

        let locator = blob.locator();
        let mut stored_size = 0_u64;
        let mut stored_hasher = sha2::Sha256::new();

        let opening = match (locator, protection) {
            (
                coven_protocol::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                coven_protocol::objects::BlobSpoolProtection::Opaque(master),
            ) => {
                let prefix = take_stored_exact(
                    &mut source,
                    &mut stored_size,
                    &mut stored_hasher,
                    KeyTag::LEN + SEALED_BLOB_HEADER_LEN,
                    locator.locator_hash(),
                )
                .await?;
                let opener = verified_sealed_blob_opener(
                    &prefix,
                    blob,
                    key_fingerprint,
                    scope,
                    &master,
                    &cloud_aad_context(store_id, &locator.semantic_key()),
                )?;
                ExactBlobOpening::Opaque {
                    opener,
                    next_chunk: 0,
                }
            }
            (
                coven_protocol::blob::locator::BlobLocator::Browsable { .. },
                coven_protocol::objects::BlobSpoolProtection::Browsable,
            ) => {
                check_stored_blob_length(blob, locator.plaintext_size())?;
                ExactBlobOpening::Browsable
            }
            (coven_protocol::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (coven_protocol::blob::locator::BlobLocator::Browsable { .. }, _) => {
                return Err(StorageError::Configuration(
                    "browsable blob locator cannot use audience encryption".to_string(),
                ));
            }
        };

        Ok(Self {
            // A sealed blob is verified by opening it: every chunk's tag covers
            // its bytes, its index, and the header that frames them, so nothing
            // the provider can serve opens as this blob's plaintext. A browsable
            // home stores the plaintext in the clear and has no tags, so there
            // the row's content hash is the only thing that can refuse the
            // provider's bytes — the two homes verify by different means, not by
            // one mechanism plus a spare.
            hasher: match opening {
                ExactBlobOpening::Browsable => Some(coven_protocol::blob::ContentHasher::default()),
                ExactBlobOpening::Opaque { .. } => None,
            },
            source,
            opening,
            remaining: locator.plaintext_size(),
            expected_hash: locator.plaintext_hash(),
            locator_hash: locator.locator_hash(),
            pending: Vec::new(),
            pending_offset: 0,
            stored_size,
            stored_hasher,
            expected_stored_size: blob.object().stored_size(),
            expected_stored_hash: blob.object().stored_hash(),
            ended: false,
        })
    }

    fn take_pending(&mut self, max: usize) -> Vec<u8> {
        let end = (self.pending_offset + max).min(self.pending.len());
        let result = self.pending[self.pending_offset..end].to_vec();
        self.pending_offset = end;
        if self.pending_offset == self.pending.len() {
            self.pending.clear();
            self.pending_offset = 0;
        }
        result
    }

    /// Take the next stored bytes through the reader's own size and hash
    /// accounting, so nothing reaches the framing uncounted.
    async fn take_stored(&mut self, max: usize) -> Result<Vec<u8>, StorageError> {
        take_stored(
            &mut self.source,
            &mut self.stored_size,
            &mut self.stored_hasher,
            max,
        )
        .await
    }

    /// The source has delivered the last byte the framing needed. Nothing may
    /// follow it, and what did pass through must be the object the reference
    /// names — only then does the reader report EOF.
    async fn finish(&mut self) -> Result<(), crate::local_file::PlaintextChunkError> {
        use sha2::Digest as _;

        // One more byte is one too many: what the source has after the framing
        // is not part of this object.
        if !self
            .take_stored(1)
            .await
            .map_err(crate::local_file::PlaintextChunkError::Remote)?
            .is_empty()
        {
            return Err(crate::local_file::PlaintextChunkError::InvalidContent(
                format!("blob {} stored body has trailing bytes", self.locator_hash),
            ));
        }
        let stored_hash = ObjectHash::from_digest(self.stored_hasher.clone().finalize().into());
        if self.stored_size != self.expected_stored_size || stored_hash != self.expected_stored_hash
        {
            return Err(crate::local_file::PlaintextChunkError::InvalidContent(
                format!(
                    "blob {} stored body is {} bytes with hash {stored_hash}, its reference names {} bytes with hash {}",
                    self.locator_hash,
                    self.stored_size,
                    self.expected_stored_size,
                    self.expected_stored_hash
                ),
            ));
        }
        self.verify_complete()?;
        self.ended = true;
        Ok(())
    }

    fn verify_complete(&mut self) -> Result<(), crate::local_file::PlaintextChunkError> {
        let Some(hasher) = self.hasher.take() else {
            return Ok(());
        };
        let actual = hasher.finish();
        if actual != self.expected_hash.to_string() {
            return Err(crate::local_file::PlaintextChunkError::InvalidContent(
                format!(
                    "blob {} plaintext hash mismatch: expected {}, got {actual}",
                    self.locator_hash, self.expected_hash
                ),
            ));
        }
        Ok(())
    }
}

/// Up to `max` stored bytes, counted and hashed into the running stored-object
/// identity. Every byte the reader takes from a source goes through here.
async fn take_stored(
    source: &mut StoredBlobSource,
    stored_size: &mut u64,
    stored_hasher: &mut sha2::Sha256,
    max: usize,
) -> Result<Vec<u8>, StorageError> {
    use sha2::Digest as _;

    let bytes = source.next_bytes(max).await?;
    *stored_size += bytes.len() as u64;
    stored_hasher.update(&bytes);
    Ok(bytes)
}

/// Exactly `len` stored bytes. A source that ends first has not served the
/// object the framing requires, which is content the caller must refuse rather
/// than a short read it can work with.
async fn take_stored_exact(
    source: &mut StoredBlobSource,
    stored_size: &mut u64,
    stored_hasher: &mut sha2::Sha256,
    len: usize,
    locator_hash: ObjectHash,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let chunk = take_stored(source, stored_size, stored_hasher, len - bytes.len()).await?;
        if chunk.is_empty() {
            return Err(StorageError::InvalidContent(format!(
                "blob {locator_hash} stored body ended after {} of {len} required bytes",
                bytes.len()
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Split a stored sealed blob into the three things its bytes declare: the key
/// fingerprint naming what sealed it, the header framing its chunks, and the
/// sealed chunks themselves.
///
/// The layout is `[KeyTag][SealedBlobHeader][chunks…]`. The cleartext key tag
/// selects the exact key fingerprint. The header includes the format version,
/// nonce policy, chunk size, plaintext length, and any stored nonce; its bytes
/// are authenticated with each chunk's context and index when the chunk opens.
pub(crate) fn split_sealed_blob(
    stored: &[u8],
) -> Result<
    (
        coven_keys::encryption::KeyFingerprint,
        SealedBlobHeader,
        &[u8],
    ),
    EncryptionError,
> {
    let (fingerprint, rest) = KeyTag::read(stored)?;
    let header = SealedBlobHeader::parse(rest)?;
    Ok((
        coven_keys::encryption::KeyFingerprint::from_bytes(fingerprint),
        header,
        &rest[header.prefix_len() as usize..],
    ))
}

/// Open a blob this layer sealed: split the prefix, then open every chunk under
/// `encryption` with the AAD context the seal was bound to. Returns the
/// fingerprint of the key that sealed it alongside the plaintext.
#[cfg(any(test, feature = "test-utils"))]
pub fn open_sealed_blob(
    stored: &[u8],
    encryption: &EncryptionService,
    aad_context: &[u8],
) -> Result<(coven_keys::encryption::KeyFingerprint, Vec<u8>), EncryptionError> {
    let (fingerprint, header, chunks) = split_sealed_blob(stored)?;
    let plaintext = encryption
        .blob_opener(
            header,
            &NoncePolicy::DerivedFromContext {
                context: aad_context.to_vec(),
            },
            aad_context,
        )?
        .open_chunks(0..header.chunk_count(), chunks)?;
    Ok((fingerprint, plaintext))
}

/// Resolve a sealed blob's `[key tag][header]` prefix into the key that sealed
/// it and the layout it declares. The fingerprint must be the one the row's
/// locator names — a blob sealed under any other key is not this row's blob,
/// whatever it decrypts to.
pub(crate) fn verified_sealed_blob_opener(
    prefix: &[u8],
    blob: &coven_protocol::blob::locator::StoredBlobRef,
    key_fingerprint: &coven_keys::encryption::KeyFingerprint,
    scope: &coven_protocol::blob::BlobScope,
    master: &EncryptionService,
    aad_context: &[u8],
) -> Result<coven_keys::encryption::SealedBlobOpener, StorageError> {
    let locator = blob.locator();
    let (fingerprint, header, _) =
        split_sealed_blob(prefix).map_err(|source| StorageError::Decryption {
            context: format!("blob {}", locator.locator_hash()),
            source,
        })?;
    if fingerprint != *key_fingerprint {
        return Err(StorageError::InvalidContent(format!(
            "blob {} stored key fingerprint differs from its locator",
            locator.locator_hash()
        )));
    }
    let encryption = opening_encryption_for_scope(scope.clone(), master, fingerprint.as_bytes())
        .map_err(|source| StorageError::Decryption {
            context: format!("blob {} audience key", locator.locator_hash()),
            source,
        })?;
    if header.plaintext_len() != locator.plaintext_size() {
        return Err(StorageError::InvalidContent(format!(
            "blob {} header declares {} plaintext bytes, its locator declares {}",
            locator.locator_hash(),
            header.plaintext_len(),
            locator.plaintext_size()
        )));
    }
    check_stored_blob_length(blob, KeyTag::LEN as u64 + header.sealed_len())?;
    encryption
        .blob_opener(
            header,
            &NoncePolicy::DerivedFromContext {
                context: aad_context.to_vec(),
            },
            aad_context,
        )
        .map_err(|source| StorageError::Decryption {
            context: format!("blob {}", locator.locator_hash()),
            source: source.into(),
        })
}

/// Check a stored blob's length against what its own framing implies. The row
/// pins the stored object's exact size, so a length the framing cannot produce
/// means the object is not the one the row names.
pub(crate) fn check_stored_blob_length(
    blob: &coven_protocol::blob::locator::StoredBlobRef,
    expected: u64,
) -> Result<(), StorageError> {
    if blob.object().stored_size() != expected {
        return Err(StorageError::InvalidContent(format!(
            "blob {} stored length is {}, expected {expected} for its locator",
            blob.locator().locator_hash(),
            blob.object().stored_size()
        )));
    }
    Ok(())
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for ExactBlobPlaintextReader {
    type Error = crate::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        max: usize,
    ) -> Result<Vec<u8>, crate::local_file::PlaintextChunkError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        // The empty result that ends a `write_plaintext` is handed out only by
        // `finish`, so a caller never mistakes a chunk that opened to zero
        // bytes — the single chunk of an empty blob — for the end of the blob.
        loop {
            if !self.pending.is_empty() {
                return Ok(self.take_pending(max));
            }
            if self.ended {
                return Ok(Vec::new());
            }

            let plaintext = match &mut self.opening {
                ExactBlobOpening::Browsable => {
                    if self.remaining == 0 {
                        self.finish().await?;
                        continue;
                    }
                    let wanted = usize::try_from(self.remaining.min(max as u64)).map_err(|_| {
                        crate::local_file::PlaintextChunkError::InvalidContent(
                            "blob plaintext read length does not fit this platform".to_string(),
                        )
                    })?;
                    let chunk = self
                        .take_stored(wanted)
                        .await
                        .map_err(crate::local_file::PlaintextChunkError::Remote)?;
                    if chunk.is_empty() {
                        return Err(crate::local_file::PlaintextChunkError::InvalidContent(
                            format!("blob {} plaintext ended early", self.locator_hash),
                        ));
                    }
                    chunk
                }
                ExactBlobOpening::Opaque { opener, next_chunk } => {
                    if *next_chunk >= opener.header().chunk_count() {
                        self.finish().await?;
                        continue;
                    }
                    let index = *next_chunk;
                    let sealed_len = usize::try_from(opener.header().sealed_chunk_len(index))
                        .map_err(|_| {
                            crate::local_file::PlaintextChunkError::InvalidContent(
                                "one sealed blob chunk does not fit this platform".to_string(),
                            )
                        })?;
                    let sealed = take_stored_exact(
                        &mut self.source,
                        &mut self.stored_size,
                        &mut self.stored_hasher,
                        sealed_len,
                        self.locator_hash,
                    )
                    .await
                    .map_err(crate::local_file::PlaintextChunkError::Remote)?;
                    let plaintext = opener.open_chunk(index, &sealed).map_err(|source| {
                        crate::local_file::PlaintextChunkError::Decryption {
                            context: format!("blob {}", self.locator_hash),
                            source: source.into(),
                        }
                    })?;
                    *next_chunk += 1;
                    plaintext
                }
            };
            if plaintext.len() as u64 > self.remaining {
                return Err(crate::local_file::PlaintextChunkError::InvalidContent(
                    format!("blob {} produced excess plaintext", self.locator_hash),
                ));
            }
            // Present only for a browsable home, where the content hash is what
            // refuses the provider's bytes; a sealed blob is refused by its tags.
            if let Some(hasher) = self.hasher.as_mut() {
                hasher.update(&plaintext);
            }
            self.remaining -= plaintext.len() as u64;
            self.pending = plaintext;
        }
    }
}
