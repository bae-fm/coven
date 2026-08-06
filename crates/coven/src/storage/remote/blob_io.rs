use super::cipher::*;
use super::*;

/// How a cloud home names its blob objects. Paired with the at-rest
/// [`CloudCipher`] by the home's [`HomeStorage`](coven_foundation::config::HomeStorage): an
/// opaque home is `Hashed` + encrypted, a browsable home is `Plain` + plaintext.
#[derive(Clone, Copy)]
pub(crate) enum BlobPathScheme {
    /// Content-addressed shard `{namespace}/{ab}/{cd}/{id}` (an opaque home).
    Hashed,
    /// The consumer's own readable path, verbatim: `{namespace}/{cloud_path}`
    /// (a browsable home). The consumer must supply `cloud_path` on every blob;
    /// coven errors otherwise.
    Plain,
}

impl BlobPathScheme {
    /// The blob-path scheme a home's storage mode selects: an opaque home
    /// obfuscates (`Hashed`), a browsable home is readable (`Plain`).
    pub(crate) fn for_storage(storage: coven_foundation::config::HomeStorage) -> Self {
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
pub(crate) struct BlobRangeReader {
    pub(super) exact: Arc<dyn ExactSlotStorage>,
    pub(super) slot: crate::protocol::objects::ObjectSlot,
    pub(super) opener: coven_keys::encryption::SealedBlobOpener,
    pub(super) plaintext_size: u64,
    pub(super) window: std::num::NonZeroU64,
}

impl BlobRangeReader {
    /// The blob's whole plaintext length, as its row declares it.
    pub(crate) fn plaintext_size(&self) -> u64 {
        self.plaintext_size
    }

    /// Read exactly `len` plaintext bytes at `offset`. A range past the blob's
    /// end is an error, never a short read.
    pub(crate) async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
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
        let chunks = header.covering_chunks(offset, end).map_err(|error| {
            StorageError::InvalidContent(format!("blob range {offset}..{end}: {error}"))
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
                StorageError::Decryption(format!("blob range {offset}..{end}: {error}"))
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

pub(super) enum ExactBlobOpening {
    Browsable,
    Opaque {
        opener: coven_keys::encryption::SealedBlobOpener,
        next_chunk: u64,
    },
}

/// Opens one already exact-verified stored blob and withholds EOF until the
/// complete plaintext size and hash match the signed locator.
pub(super) struct ExactBlobPlaintextReader {
    source: crate::storage::local_file::PlaintextReader,
    opening: ExactBlobOpening,
    remaining: u64,
    hasher: Option<crate::protocol::blob::ContentHasher>,
    expected_hash: ObjectHash,
    locator_hash: ObjectHash,
    pending: Vec<u8>,
    pending_offset: usize,
}

impl ExactBlobPlaintextReader {
    pub(super) async fn new(
        stored_file: &Path,
        store_id: &str,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
    ) -> Result<Self, StorageError> {
        let locator = blob.locator();
        let mut source = crate::storage::local_file::open_reader(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;

        let opening = match (locator, protection) {
            (
                crate::protocol::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                crate::protocol::objects::BlobSpoolProtection::Opaque(master),
            ) => {
                let prefix = read_source_exact(
                    &mut source,
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
                crate::protocol::blob::locator::BlobLocator::Browsable { .. },
                crate::protocol::objects::BlobSpoolProtection::Browsable,
            ) => {
                check_stored_blob_length(blob, locator.plaintext_size())?;
                ExactBlobOpening::Browsable
            }
            (crate::protocol::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (crate::protocol::blob::locator::BlobLocator::Browsable { .. }, _) => {
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
                ExactBlobOpening::Browsable => {
                    Some(crate::protocol::blob::ContentHasher::default())
                }
                ExactBlobOpening::Opaque { .. } => None,
            },
            source,
            opening,
            remaining: locator.plaintext_size(),
            expected_hash: locator.plaintext_hash(),
            locator_hash: locator.locator_hash(),
            pending: Vec::new(),
            pending_offset: 0,
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

    fn verify_complete(&mut self) -> Result<(), crate::storage::local_file::PlaintextChunkError> {
        let Some(hasher) = self.hasher.take() else {
            return Ok(());
        };
        let actual = hasher.finish();
        if actual != self.expected_hash.to_string() {
            return Err(
                crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                    "blob {} plaintext hash mismatch: expected {}, got {actual}",
                    self.locator_hash, self.expected_hash
                )),
            );
        }
        Ok(())
    }
}

/// Split a stored sealed blob into the three things its bytes declare: the key
/// fingerprint naming what sealed it, the header framing its chunks, and the
/// sealed chunks themselves.
///
/// The layout is `[CKF1][fingerprint: 32][version: 1][chunk_size: 4][plaintext_len: 8][chunks…]`.
/// Everything before the chunks is cleartext — a reader must know the key and the
/// chunk size before it can open anything — and all of it is bound into every
/// chunk's AAD, so a rewritten prefix fails the first open rather than re-framing
/// the object.
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
    let header = SealedBlobHeader::parse(rest)
        .map_err(|error| EncryptionError::Decryption(error.to_string()))?;
    Ok((
        coven_keys::encryption::KeyFingerprint::from_bytes(fingerprint),
        header,
        &rest[header.prefix_len() as usize..],
    ))
}

/// Resolve a sealed blob's `[key tag][header]` prefix into the key that sealed
/// it and the layout it declares. The fingerprint must be the one the row's
/// locator names — a blob sealed under any other key is not this row's blob,
/// whatever it decrypts to.
pub(super) fn verified_sealed_blob_opener(
    prefix: &[u8],
    blob: &crate::protocol::blob::locator::StoredBlobRef,
    key_fingerprint: &coven_keys::encryption::KeyFingerprint,
    scope: &crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    aad_context: &[u8],
) -> Result<coven_keys::encryption::SealedBlobOpener, StorageError> {
    let locator = blob.locator();
    let (fingerprint, header, _) = split_sealed_blob(prefix).map_err(|error| {
        StorageError::Decryption(format!("blob {}: {error}", locator.locator_hash()))
    })?;
    if fingerprint != *key_fingerprint {
        return Err(StorageError::InvalidContent(format!(
            "blob {} stored key fingerprint differs from its locator",
            locator.locator_hash()
        )));
    }
    let encryption = opening_encryption_for_scope(scope.clone(), master, fingerprint.as_bytes())
        .map_err(|error| {
            StorageError::Decryption(format!(
                "blob {} audience key: {error}",
                locator.locator_hash()
            ))
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
        .map_err(|error| {
            StorageError::Decryption(format!("blob {}: {error}", locator.locator_hash()))
        })
}

/// Check a stored blob's length against what its own framing implies. The row
/// pins the stored object's exact size, so a length the framing cannot produce
/// means the object is not the one the row names.
pub(super) fn check_stored_blob_length(
    blob: &crate::protocol::blob::locator::StoredBlobRef,
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
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        if !self.pending.is_empty() {
            return Ok(self.take_pending(max));
        }
        if self.remaining == 0 {
            self.verify_complete()?;
            return Ok(Vec::new());
        }

        let plaintext = match &mut self.opening {
            ExactBlobOpening::Browsable => {
                let wanted = usize::try_from(self.remaining.min(max as u64)).map_err(|_| {
                    crate::storage::local_file::PlaintextChunkError::InvalidContent(
                        "blob plaintext read length does not fit this platform".to_string(),
                    )
                })?;
                let chunk = self.source.next_chunk(wanted).await.map_err(|error| {
                    crate::storage::local_file::PlaintextChunkError::Local(error.to_string())
                })?;
                if chunk.is_empty() {
                    return Err(
                        crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                            "blob {} plaintext ended early",
                            self.locator_hash
                        )),
                    );
                }
                chunk
            }
            ExactBlobOpening::Opaque { opener, next_chunk } => {
                let index = *next_chunk;
                let sealed_len =
                    usize::try_from(opener.header().sealed_chunk_len(index)).map_err(|_| {
                        crate::storage::local_file::PlaintextChunkError::InvalidContent(
                            "one sealed blob chunk does not fit this platform".to_string(),
                        )
                    })?;
                let sealed = read_source_exact(&mut self.source, sealed_len, self.locator_hash)
                    .await
                    .map_err(crate::storage::local_file::PlaintextChunkError::Remote)?;
                let plaintext = opener.open_chunk(index, &sealed).map_err(|error| {
                    crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                        "blob {}: {error}",
                        self.locator_hash
                    ))
                })?;
                *next_chunk += 1;
                plaintext
            }
        };
        if plaintext.len() as u64 > self.remaining {
            return Err(
                crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                    "blob {} produced excess plaintext",
                    self.locator_hash
                )),
            );
        }
        // Present only for a browsable home, where the content hash is what
        // refuses the provider's bytes; a sealed blob is refused by its tags.
        if let Some(hasher) = self.hasher.as_mut() {
            hasher.update(&plaintext);
        }
        self.remaining -= plaintext.len() as u64;
        self.pending = plaintext;
        Ok(self.take_pending(max))
    }
}
