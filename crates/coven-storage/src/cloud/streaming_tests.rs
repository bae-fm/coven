use super::*;
use coven_keys::encryption::EncryptionService;
const CHUNK_SIZE: usize = DEFAULT_BLOB_CHUNK_SIZE.get() as usize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

/// A part sink that records what the multipart driver hands it, so a test can
/// assert the order, the abort, and the assembled object.
struct RecordingSink {
    store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    abort_calls: Arc<AtomicUsize>,
    key: String,
    buf: Vec<u8>,
}

impl RecordingSink {
    fn new(
        key: &str,
        store: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
        abort_calls: &Arc<AtomicUsize>,
    ) -> Self {
        RecordingSink {
            store: Arc::clone(store),
            abort_calls: Arc::clone(abort_calls),
            key: key.to_string(),
            buf: Vec::new(),
        }
    }
}

#[async_trait]
impl PartSink for RecordingSink {
    fn part_size(&self) -> usize {
        4 * 1024 * 1024
    }
    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        _is_last: bool,
        _control: &UploadControl,
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
        self.abort_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        self.store.lock().unwrap().insert(self.key, self.buf);
        Ok(())
    }
}

/// A large blob streams through the multipart driver, round-trips exactly, and
/// reports monotonic progress reaching the full length.
#[tokio::test]
async fn multipart_streams_a_large_blob_with_monotonic_progress() {
    let store = Arc::new(Mutex::new(HashMap::new()));
    let abort_calls = Arc::new(AtomicUsize::new(0));
    let data: Vec<u8> = (0..20_000_003u32).map(|i| (i % 251) as u8).collect();
    let ticks = Arc::new(Mutex::new(Vec::<u64>::new()));
    let recorded = Arc::clone(&ticks);
    let progress: UploadProgress = Arc::new(move |n: u64| recorded.lock().unwrap().push(n));
    let control = UploadControl::running(progress);

    MultipartUpload::new(
        "k",
        BlobBody::from_bytes(data.clone()),
        Box::new(RecordingSink::new("k", &store, &abort_calls)),
        &control,
    )
    .run()
    .await
    .unwrap();

    assert_eq!(
        store.lock().unwrap().get("k").cloned().unwrap(),
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
async fn multipart_aborts_when_the_body_ends_before_its_declared_length() {
    let store = Arc::new(Mutex::new(HashMap::new()));
    let abort_calls = Arc::new(AtomicUsize::new(0));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.bin");
    std::fs::write(&path, [7; 4]).unwrap();
    let reader = crate::local_file::open_reader(&path).await.unwrap();
    let body = BlobBody::from_file_with_prefix(5, reader, None, Vec::new());
    let control = UploadControl::running(no_progress());

    let error = MultipartUpload::new(
        "short",
        body,
        Box::new(RecordingSink::new("short", &store, &abort_calls)),
        &control,
    )
    .run()
    .await
    .expect_err("an incomplete body must not commit");

    assert!(
        error.to_string().contains("ended after 4 of 5 bytes"),
        "{error}"
    );
    assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
    assert!(!store.lock().unwrap().contains_key("short"));
}

struct FailingPartSink {
    abort_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl PartSink for FailingPartSink {
    fn part_size(&self) -> usize {
        2
    }

    async fn send_part(
        &mut self,
        _part: Bytes,
        _offset: u64,
        _is_last: bool,
        _control: &UploadControl,
    ) -> Result<(), CloudHomeError> {
        Err(CloudHomeError::Transport(
            "injected part failure".to_string(),
        ))
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.abort_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Transport(
            "injected abort failure".to_string(),
        ))
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        panic!("a failed part must not finish")
    }
}

#[tokio::test]
async fn multipart_aborts_and_preserves_cleanup_failure_when_a_part_fails() {
    let abort_calls = Arc::new(AtomicUsize::new(0));
    let control = UploadControl::running(no_progress());

    let error = MultipartUpload::new(
        "part-failure",
        BlobBody::from_bytes(vec![1, 2, 3]),
        Box::new(FailingPartSink {
            abort_calls: Arc::clone(&abort_calls),
        }),
        &control,
    )
    .run()
    .await
    .expect_err("a failed multipart part must abort its session");

    assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
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
