use super::*;
use coven_keys::encryption::EncryptionService;
const CHUNK_SIZE: usize = DEFAULT_BLOB_CHUNK_SIZE.get() as usize;
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
    let reader = crate::local_file::open_reader(&path).await.unwrap();
    let policy = coven_keys::encryption::NoncePolicy::DerivedFromContext {
        context: b"storage-cloud-test".to_vec(),
    };
    let header = coven_keys::encryption::SealedBlobHeader::new(
        coven_keys::encryption::DEFAULT_BLOB_CHUNK_SIZE,
        plaintext.len() as u64,
        &policy,
    );
    let body = BlobBody::from_file_with_prefix(
        header.sealed_len(),
        reader,
        Some(
            service
                .blob_sealer(header, &policy, b"storage-cloud-test")
                .expect("the header records the policy it was built under"),
        ),
        header.to_bytes(),
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
            let header = coven_keys::encryption::SealedBlobHeader::parse(&sealed).unwrap();
            assert_eq!(header.plaintext_len(), len as u64);
            assert_eq!(
                service
                    .blob_opener(
                        header,
                        &coven_keys::encryption::NoncePolicy::DerivedFromContext {
                            context: b"storage-cloud-test".to_vec(),
                        },
                        b"storage-cloud-test",
                    )
                    .unwrap()
                    .open_chunks(
                        0..header.chunk_count(),
                        &sealed[header.prefix_len() as usize..],
                    )
                    .unwrap(),
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

    let reader = crate::local_file::open_reader(&path).await.unwrap();
    let streamed = drain(
        BlobBody::from_file_with_prefix(plaintext.len() as u64, reader, None, Vec::new()),
        4096,
    )
    .await;
    assert_eq!(streamed, plaintext);

    let reader = crate::local_file::open_reader(&path).await.unwrap();
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
    abort_calls: AtomicUsize,
    threshold: u64,
}

impl RecordingHome {
    fn new(threshold: u64) -> Self {
        RecordingHome {
            store: Mutex::new(HashMap::new()),
            put_calls: AtomicUsize::new(0),
            multipart_calls: AtomicUsize::new(0),
            abort_calls: AtomicUsize::new(0),
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
    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.home.abort_calls.fetch_add(1, Ordering::SeqCst);
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
    async fn set_access(
        &self,
        _desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
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

#[tokio::test]
async fn write_blob_aborts_when_the_body_ends_before_its_declared_length() {
    let home = RecordingHome::new(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.bin");
    std::fs::write(&path, [7; 4]).unwrap();
    let reader = crate::local_file::open_reader(&path).await.unwrap();
    let body = BlobBody::from_file_with_prefix(5, reader, None, Vec::new());

    let error = home
        .write("short", body, &no_progress())
        .await
        .expect_err("an incomplete body must not commit");

    assert!(
        error.to_string().contains("ended after 4 of 5 bytes"),
        "{error}"
    );
    assert_eq!(home.abort_calls.load(Ordering::SeqCst), 1);
    assert!(!home.store.lock().unwrap().contains_key("short"));
}

struct FailingPartHome {
    abort_calls: AtomicUsize,
}

struct FailingPartSink<'a> {
    home: &'a FailingPartHome,
}

#[async_trait]
impl PartSink for FailingPartSink<'_> {
    fn part_size(&self) -> usize {
        2
    }

    async fn send_part(
        &mut self,
        _part: Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        Err(CloudHomeError::Transport(
            "injected part failure".to_string(),
        ))
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.home.abort_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Transport(
            "injected abort failure".to_string(),
        ))
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        panic!("a failed part must not finish")
    }
}

#[async_trait]
impl CloudHome for FailingPartHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        panic!("multipart test must not use put_object")
    }

    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        Ok(Box::new(FailingPartSink { home: self }))
    }

    fn multipart_threshold(&self) -> u64 {
        1
    }

    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!()
    }

    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
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

    async fn set_access(
        &self,
        _desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        unimplemented!()
    }
}

#[tokio::test]
async fn write_blob_aborts_and_preserves_cleanup_failure_when_a_part_fails() {
    let home = FailingPartHome {
        abort_calls: AtomicUsize::new(0),
    };

    let error = home
        .write(
            "part-failure",
            BlobBody::from_bytes(vec![1, 2, 3]),
            &no_progress(),
        )
        .await
        .expect_err("a failed multipart part must abort its session");

    assert_eq!(home.abort_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(error, CloudHomeError::CleanupFailed { .. }));
    assert!(
        error.to_string().contains("injected part failure"),
        "{error}"
    );
    assert!(
        error.to_string().contains("injected abort failure"),
        "{error}"
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
