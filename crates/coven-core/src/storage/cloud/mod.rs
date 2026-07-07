//! CloudHome: low-level cloud storage abstraction.
//!
//! Each backend (S3, R2, B2, etc.) implements `CloudHome` -- 8 methods for
//! raw bytes in/out. No encryption, no path layout knowledge, no sync
//! semantics. Higher-level concerns live in `CloudSyncStorage` which wraps any
//! `dyn CloudHome` and applies the path layout and at-rest protection.

// Pure helpers that S3-compatible backends share.
pub mod s3_common;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::encryption::{ChunkSealer, CHUNK_SIZE};
use crate::local_blob::PlaintextReader;

/// Errors from raw cloud storage operations.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Information needed to join a cloud home from another device.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CloudHomeJoinInfo {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        #[serde(default)]
        key_prefix: Option<String>,
    },
    GoogleDrive {
        folder_id: String,
    },
    Dropbox {
        shared_folder_id: String,
    },
    OneDrive {
        drive_id: String,
        folder_id: String,
    },
    CloudKit,
    CloudKitShare {
        share_url: String,
        owner_name: String,
        zone_name: String,
    },
}

impl CloudHomeJoinInfo {
    pub fn cloud_provider(&self) -> crate::config::CloudProvider {
        use crate::config::CloudProvider;
        match self {
            CloudHomeJoinInfo::S3 { .. } => CloudProvider::S3,
            CloudHomeJoinInfo::GoogleDrive { .. } => CloudProvider::GoogleDrive,
            CloudHomeJoinInfo::Dropbox { .. } => CloudProvider::Dropbox,
            CloudHomeJoinInfo::OneDrive { .. } => CloudProvider::OneDrive,
            CloudHomeJoinInfo::CloudKit | CloudHomeJoinInfo::CloudKitShare { .. } => {
                CloudProvider::CloudKit
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudAccessGrant {
    pub member_pubkey: String,
    pub provider_account_email: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudAccessRevoke {
    pub member_pubkey: String,
    pub provider_account_email: Option<String>,
}

/// Whether a backend actually withdrew a removed member's storage credential.
///
/// Consumer clouds unshare the folder and report [`RevokeOutcome::Revoked`].
/// Shared-credential backends (S3) hand out one static bucket key that cannot be
/// withdrawn from a single member and report [`RevokeOutcome::Unsupported`].
/// Removal proceeds either way: revoking chain membership and rotating the
/// library key — not withdrawing the credential — is what protects post-removal
/// content, so `Unsupported` is a truthful outcome, not a failure to paper over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevokeOutcome {
    Revoked,
    Unsupported,
}

impl CloudAccessGrant {
    pub fn require_provider_email(&self, provider: &str) -> Result<&str, CloudHomeError> {
        require_provider_email(provider, self.provider_account_email.as_deref())
    }
}

impl CloudAccessRevoke {
    pub fn require_provider_email(&self, provider: &str) -> Result<&str, CloudHomeError> {
        require_provider_email(provider, self.provider_account_email.as_deref())
    }
}

fn require_provider_email<'a>(
    provider: &str,
    email: Option<&'a str>,
) -> Result<&'a str, CloudHomeError> {
    match email {
        Some(email) if !email.is_empty() => Ok(email),
        _ => Err(CloudHomeError::Storage(format!(
            "{provider} sharing requires the invitee's provider account email"
        ))),
    }
}

/// The HTTP `Range` header value for a ranged GET. `start` is inclusive and
/// `end` is exclusive (the `CloudHome` contract); the header is inclusive on
/// both ends, so the upper bound is `end - 1`. The one definition every backend
/// — both S3 transports and the OAuth REST backends — uses.
pub fn range_header(start: u64, end: u64) -> String {
    format!("bytes={start}-{}", end.saturating_sub(1))
}

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
pub fn no_progress() -> impl Fn(u64) + Send + Sync {
    |_| {}
}

/// A blob as a **sized stream of already-final bytes**: sealed chunks for an
/// encrypted home, plaintext for a browsable one. Encryption-agnostic and
/// concrete (no `dyn Stream`, so it sidesteps the native-`Send` / wasm-`?Send`
/// split). [`next_part`](BlobBody::next_part) hands the bytes to a streaming
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
    /// sealed one 64 KiB chunk at a time into the `[base_nonce][chunk]...` form.
    File {
        reader: PlaintextReader,
        /// Final bytes emitted before the nonce/plaintext stream.
        prefix: Bytes,
        /// `Some` seals each chunk under the scope's key; `None` passes the
        /// plaintext through (a browsable home).
        sealer: Option<ChunkSealer>,
        /// The base nonce still owed before the first sealed chunk (encrypted only).
        nonce_pending: bool,
        /// Whether any plaintext chunk has been sealed — distinguishes a truly
        /// empty file (which still seals one tag-only chunk, matching
        /// `EncryptionService::encrypt`) from one that produced chunks and then
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
                nonce_pending,
                sealed_any,
                eof,
            } => {
                if !prefix.is_empty() {
                    return Ok(Some(std::mem::take(prefix)));
                }
                if *nonce_pending {
                    *nonce_pending = false;
                    if let Some(s) = sealer {
                        return Ok(Some(Bytes::copy_from_slice(&s.base_nonce())));
                    }
                }
                if *eof {
                    return Ok(None);
                }
                let chunk = reader
                    .next_chunk(CHUNK_SIZE)
                    .await
                    .map_err(CloudHomeError::Storage)?;
                if chunk.is_empty() {
                    *eof = true;
                    // A sealed empty file still emits one tag-only chunk, matching
                    // `EncryptionService::encrypt`; a plaintext file (or a sealed
                    // one that already produced chunks) ends here.
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

    pub(crate) fn from_file_with_prefix(
        len: u64,
        reader: PlaintextReader,
        sealer: Option<ChunkSealer>,
        prefix: Vec<u8>,
    ) -> Self {
        let nonce_pending = sealer.is_some();
        BlobBody {
            len,
            source: BlobSource::File {
                reader,
                prefix: Bytes::from(prefix),
                sealer,
                nonce_pending,
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
}

/// The one per-provider streaming-upload surface: a session that accepts ordered
/// parts and commits. The central [`write_blob`] driver opens one of these for a
/// large blob and pumps [`BlobBody`] parts into it — no backend writes its own
/// upload loop, collect, or progress call.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PartSink {
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

    /// Commit the upload (e.g. S3 `complete_multipart_upload`); a no-op where the
    /// last `send_part` already committed.
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError>;
}

/// A boxed [`PartSink`] borrowing its home for `'a`. Carries `Send` on native
/// (the home is multi-threaded) and drops it on wasm (single-threaded browser).
#[cfg(not(target_arch = "wasm32"))]
pub type BoxPartSink<'a> = Box<dyn PartSink + Send + 'a>;
#[cfg(target_arch = "wasm32")]
pub type BoxPartSink<'a> = Box<dyn PartSink + 'a>;

/// The central upload driver: pick single-request vs multipart by size and pump
/// the parts. A blob at or below the home's `multipart_threshold` goes up as one
/// bounded `put_object`; a larger one opens a multipart/resumable session and
/// streams [`BlobBody`] parts into it, reporting cumulative progress. The trait's
/// `write` is this; no backend overrides it.
async fn write_blob<C: CloudHome + ?Sized>(
    home: &C,
    key: &str,
    mut body: BlobBody,
    progress: &UploadProgress<'_>,
) -> Result<(), CloudHomeError> {
    if body.len() <= home.multipart_threshold() {
        let data = body.collect().await?;
        let n = data.len() as u64;
        home.put_object(key, data).await?;
        progress(n);
        return Ok(());
    }
    let mut sink = home.open_multipart(key, body.len()).await?;
    let part_size = sink.part_size();
    let total = body.len();
    let mut offset = 0u64;
    while let Some(part) = body.next_part(part_size).await? {
        let n = part.len() as u64;
        let is_last = offset + n >= total;
        sink.send_part(part, offset, is_last).await?;
        offset += n;
        progress(offset);
    }
    sink.finish().await
}

/// Low-level cloud storage. Implementations handle a single library.
///
/// All methods deal in raw bytes. No encryption or path layout logic.
///
/// The trait carries `Send + Sync` (and `Send` method futures) on native, where
/// the DB lives on a thread actor and backends await multi-threaded SDKs, and
/// drops them on wasm, where the browser is single-threaded, reqwest's `Response`
/// is `!Send`, and the engine drives every future on that one thread. The
/// supertrait bound is the cfg'd [`crate::MaybeThreadSafe`] marker and the
/// futures are cfg'd by `async_trait`'s `?Send` mode on wasm.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait CloudHome: crate::MaybeThreadSafe {
    /// Verify the backend is reachable with the configured credentials.
    /// Setup flows call this *before* persisting credentials, so a typo or
    /// missing bucket fails fast at setup time instead of via a delayed
    /// reconnect banner. Default implementation issues a no-op list against
    /// a sentinel prefix — backends override with cheaper provider-specific
    /// auth checks (e.g. S3 HeadBucket) where available.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.list("__coven_probe__").await.map(drop)
    }

    /// One bounded single-request upload, creating or overwriting `key`. Used only
    /// for blobs at or below [`multipart_threshold`](CloudHome::multipart_threshold);
    /// large blobs stream through [`open_multipart`](CloudHome::open_multipart).
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;

    /// Open a streaming multipart/resumable upload for `total_len` bytes, returning
    /// the [`PartSink`] the driver pumps ordered parts into.
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError>;

    /// Blobs at or below this size go via [`put_object`](CloudHome::put_object);
    /// larger ones stream via [`open_multipart`](CloudHome::open_multipart).
    fn multipart_threshold(&self) -> u64;

    /// Write a sized [`BlobBody`] to `key`. Not overridden — the central
    /// [`write_blob`] driver picks single-request vs multipart and pumps the
    /// parts, reporting cumulative bytes through `progress` for the per-file bar.
    async fn write(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        write_blob(self, key, body, progress).await
    }

    /// Read the full contents of a key.
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;

    /// Read a byte range from a key. `start` is inclusive, `end` is exclusive.
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError>;

    /// List all keys under a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;

    /// Delete a key. Not an error if the key does not exist.
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;

    /// Check whether a key exists.
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;

    /// Grant access to a member and return connection info for the cloud home.
    /// Consumer-cloud backends share the folder with the member's account.
    /// Shared-credential backends (S3) return the bucket credentials directly.
    /// Those credentials cannot be withdrawn from one member later, so the
    /// confidentiality of content written after a removal rests on the library
    /// key rotation the caller performs when revoking membership, not on making
    /// the removed member's credential stop working.
    async fn grant_access(
        &self,
        grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError>;

    /// Revoke a member's provider-level access to the cloud home. Member removal
    /// always revokes chain membership and rotates the library key; this call is
    /// the additional, provider-dependent step of withdrawing the storage
    /// credential. Consumer clouds unshare the folder and return
    /// [`RevokeOutcome::Revoked`]. Shared-credential backends (S3) cannot
    /// withdraw one member's copy of a static bucket key and return
    /// [`RevokeOutcome::Unsupported`]; removal still completes because the key
    /// rotation, not this call, protects post-removal content. An `Err` aborts
    /// the removal, so a backend that offers no per-member revocation reports
    /// `Unsupported` rather than erroring.
    async fn revoke_access(
        &self,
        revoke: CloudAccessRevoke,
    ) -> Result<RevokeOutcome, CloudHomeError>;
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::encryption::{EncryptionService, CHUNK_SIZE};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn service() -> EncryptionService {
        EncryptionService::from_key([7u8; 32])
    }

    /// Build a sealed [`BlobBody`] over a temp file holding `plaintext`. The
    /// returned `TempDir` keeps the file alive for the reader's life.
    async fn sealed_body(
        service: &EncryptionService,
        plaintext: &[u8],
    ) -> (tempfile::TempDir, BlobBody) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, plaintext).unwrap();
        let reader = crate::local_blob::open_reader(&path).await.unwrap();
        let body = BlobBody::from_file_with_prefix(
            crate::encryption::chunked_encrypted_len(plaintext.len() as u64),
            reader,
            Some(service.sealer(plaintext.len() as u64, b"storage-cloud-test")),
            Vec::new(),
        );
        (dir, body)
    }

    /// Drain a body via `next_part(min)`, concatenating every part.
    async fn drain(mut body: BlobBody, min: usize) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(part) = body.next_part(min).await.unwrap() {
            // Every part but the last is exactly `min` bytes.
            out.extend_from_slice(&part);
        }
        out
    }

    /// A sealed body's concatenated `next_part` output decrypts to the original
    /// plaintext, across the chunk boundaries that matter and several part sizes.
    #[tokio::test]
    async fn sealed_body_streams_then_decrypts() {
        let service = service();
        for &len in &[
            0usize,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            200_000,
        ] {
            let plaintext: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            for &min in &[1usize, 100, CHUNK_SIZE, CHUNK_SIZE + 13, 1 << 20] {
                let (_dir, body) = sealed_body(&service, &plaintext).await;
                let expected_len = body.len();
                let sealed = drain(body, min).await;
                assert_eq!(
                    sealed.len() as u64,
                    expected_len,
                    "streamed length wrong for len={len} min={min}"
                );
                assert_eq!(
                    service.decrypt(&sealed, b"storage-cloud-test").unwrap(),
                    plaintext,
                    "sealed stream failed to round-trip for len={len} min={min}"
                );
            }
        }
    }

    /// Every non-final part is exactly `part_size`; the last is the remainder.
    #[tokio::test]
    async fn next_part_returns_exact_part_sizes() {
        let service = service();
        let plaintext = vec![0u8; CHUNK_SIZE * 3 + 17];
        let (_dir, mut body) = sealed_body(&service, &plaintext).await;
        let part_size = 1 << 20;
        let total = body.len();
        let mut offset = 0u64;
        while let Some(part) = body.next_part(part_size).await.unwrap() {
            offset += part.len() as u64;
            if offset < total {
                assert_eq!(
                    part.len(),
                    part_size,
                    "a non-final part must be exactly part_size"
                );
            } else {
                assert!(part.len() <= part_size, "the last part is the remainder");
            }
        }
        assert_eq!(offset, total);
    }

    /// `collect()` yields the same bytes as the concatenated `next_part` output —
    /// shown on a deterministic plaintext (passthrough) body so the two bodies
    /// produce identical bytes.
    #[tokio::test]
    async fn collect_equals_next_part_concatenation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        let plaintext: Vec<u8> = (0..200_003u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &plaintext).unwrap();

        let reader = crate::local_blob::open_reader(&path).await.unwrap();
        let streamed = drain(
            BlobBody::from_file_with_prefix(plaintext.len() as u64, reader, None, Vec::new()),
            4096,
        )
        .await;
        assert_eq!(streamed, plaintext);

        let reader = crate::local_blob::open_reader(&path).await.unwrap();
        let collected =
            BlobBody::from_file_with_prefix(plaintext.len() as u64, reader, None, Vec::new())
                .collect()
                .await
                .unwrap();
        assert_eq!(collected, plaintext);
        assert_eq!(collected, streamed);
    }

    /// A test home recording which upload path each write took and assembling the
    /// streamed parts so a multipart upload round-trips like a single PUT.
    struct RecordingHome {
        store: Mutex<HashMap<String, Vec<u8>>>,
        put_calls: AtomicUsize,
        multipart_calls: AtomicUsize,
        threshold: u64,
    }

    impl RecordingHome {
        fn new(threshold: u64) -> Self {
            RecordingHome {
                store: Mutex::new(HashMap::new()),
                put_calls: AtomicUsize::new(0),
                multipart_calls: AtomicUsize::new(0),
                threshold,
            }
        }
    }

    struct RecordingSink<'a> {
        home: &'a RecordingHome,
        key: String,
        buf: Vec<u8>,
    }

    #[async_trait]
    impl PartSink for RecordingSink<'_> {
        fn part_size(&self) -> usize {
            4 * 1024 * 1024
        }
        async fn send_part(
            &mut self,
            part: Bytes,
            offset: u64,
            _is_last: bool,
        ) -> Result<(), CloudHomeError> {
            assert_eq!(
                offset,
                self.buf.len() as u64,
                "parts arrive in order at the running offset"
            );
            self.buf.extend_from_slice(&part);
            Ok(())
        }
        async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
            self.home.store.lock().unwrap().insert(self.key, self.buf);
            Ok(())
        }
    }

    #[async_trait]
    impl CloudHome for RecordingHome {
        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            self.store.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }
        async fn open_multipart<'a>(
            &'a self,
            key: &str,
            _total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            self.multipart_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(RecordingSink {
                home: self,
                key: key.to_string(),
                buf: Vec::new(),
            }))
        }
        fn multipart_threshold(&self) -> u64 {
            self.threshold
        }
        async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
        }
        async fn read_range(&self, _k: &str, _s: u64, _e: u64) -> Result<Vec<u8>, CloudHomeError> {
            unimplemented!()
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            unimplemented!()
        }
        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            unimplemented!()
        }
        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            unimplemented!()
        }
        async fn grant_access(
            &self,
            _grant: CloudAccessGrant,
        ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
            unimplemented!()
        }
        async fn revoke_access(
            &self,
            _revoke: CloudAccessRevoke,
        ) -> Result<RevokeOutcome, CloudHomeError> {
            unimplemented!()
        }
    }

    /// A blob above the threshold streams through multipart, round-trips exactly,
    /// and reports monotonic progress reaching the full length.
    #[tokio::test]
    async fn write_blob_streams_large_blob_with_monotonic_progress() {
        let home = RecordingHome::new(8 * 1024 * 1024);
        let data: Vec<u8> = (0..20_000_003u32).map(|i| (i % 251) as u8).collect();
        let ticks = Mutex::new(Vec::<u64>::new());
        let progress = |n: u64| ticks.lock().unwrap().push(n);

        home.write("k", BlobBody::from_bytes(data.clone()), &progress)
            .await
            .unwrap();

        assert_eq!(home.multipart_calls.load(Ordering::SeqCst), 1);
        assert_eq!(home.put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            home.read("k").await.unwrap(),
            data,
            "multipart upload round-trips"
        );

        let ticks = ticks.lock().unwrap();
        assert!(ticks.len() >= 2, "several progress ticks: {ticks:?}");
        for w in ticks.windows(2) {
            assert!(w[1] >= w[0], "progress went backwards: {ticks:?}");
        }
        assert_eq!(
            *ticks.last().unwrap(),
            data.len() as u64,
            "progress reaches the full length"
        );
    }

    /// A blob at or below the threshold goes through `put_object` as one request.
    #[tokio::test]
    async fn write_blob_uses_put_object_below_threshold() {
        let home = RecordingHome::new(8 * 1024 * 1024);
        let data = vec![3u8; 1024];
        let total = Mutex::new(0u64);
        let progress = |n: u64| *total.lock().unwrap() = n;

        home.write("small", BlobBody::from_bytes(data.clone()), &progress)
            .await
            .unwrap();

        assert_eq!(home.put_calls.load(Ordering::SeqCst), 1);
        assert_eq!(home.multipart_calls.load(Ordering::SeqCst), 0);
        assert_eq!(home.read("small").await.unwrap(), data);
        assert_eq!(*total.lock().unwrap(), data.len() as u64);
    }
}
