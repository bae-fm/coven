//! CloudKit-backed `CloudHome` implementation.
//!
//! CloudKit's CKAsset has a 50MB limit, so large files are split into 10MB
//! chunks stored as separate records: `key.part0`, `key.part1`, etc.
//!
//! The `CloudKitOps` trait defines synchronous record operations that are
//! implemented in Swift via a UniFFI callback interface. `CloudKitCloudHome`
//! wraps these ops, adds chunking logic, and implements `CloudHome`.

use std::sync::Arc;

use async_trait::async_trait;

use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// Synchronous interface for raw CloudKit record operations.
/// Implemented in Swift via UniFFI callback interface.
/// Methods block the calling thread while CloudKit async operations complete.
pub trait CloudKitOps: Send + Sync {
    fn write_record(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;
    fn read_record(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    fn list_records(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
    fn delete_record(&self, key: &str) -> Result<(), CloudHomeError>;
    fn record_exists(&self, key: &str) -> Result<bool, CloudHomeError>;
    fn grant_access(&self, email: &str) -> Result<String, CloudHomeError>;
    fn revoke_access(&self, user_record_id: &str) -> Result<(), CloudHomeError>;
    fn accept_share(&self, share_url: &str) -> Result<(), CloudHomeError>;
}

/// CloudKit-backed cloud home with automatic chunking for large files.
pub struct CloudKitCloudHome {
    ops: Arc<dyn CloudKitOps>,
}

impl CloudKitCloudHome {
    pub fn new(ops: Arc<dyn CloudKitOps>) -> Self {
        Self { ops }
    }
}

/// If a key ends with `.part{digits}`, strip that suffix to get the base key.
fn strip_part_suffix(key: &str) -> &str {
    if let Some(idx) = key.rfind(".part") {
        let after = &key[idx + 5..];
        if !after.is_empty() && after.chars().all(|c| c.is_ascii_digit()) {
            return &key[..idx];
        }
    }
    key
}

/// Delete old single record and chunk records for a key (best-effort).
fn delete_all_variants(ops: &dyn CloudKitOps, key: &str) -> Result<(), CloudHomeError> {
    // Delete single record (ignore not-found)
    match ops.delete_record(key) {
        Ok(()) | Err(CloudHomeError::NotFound(_)) => {}
        Err(e) => return Err(e),
    }

    // Delete all chunk records
    let chunk_prefix = format!("{key}.part");
    let chunks = ops.list_records(&chunk_prefix)?;
    for chunk_key in chunks {
        match ops.delete_record(&chunk_key) {
            Ok(()) | Err(CloudHomeError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

/// A [`PartSink`] over CloudKit's chunked record layout: each `send_part` writes
/// one `{key}.part{i}` record (CKAsset caps at 50 MB, so a large blob is split),
/// `finish` is a no-op. The existing records were cleared by `open_multipart`
/// before the first part.
struct CloudKitPartSink {
    ops: Arc<dyn CloudKitOps>,
    key: String,
    index: usize,
}

#[async_trait]
impl super::PartSink for CloudKitPartSink {
    fn part_size(&self) -> usize {
        CHUNK_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let i = self.index;
        self.index += 1;
        let ops = self.ops.clone();
        let chunk_key = format!("{}.part{i}", self.key);
        let data = part.to_vec();
        tokio::task::spawn_blocking(move || ops.write_record(&chunk_key, data))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

#[async_trait]
impl CloudHome for CloudKitCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        // Clean up any existing single or chunked records first (an overwrite may
        // transition between single and chunked layouts).
        let ops = self.ops.clone();
        let k = key.to_string();
        tokio::task::spawn_blocking(move || delete_all_variants(&*ops, &k))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))??;

        let ops = self.ops.clone();
        let k = key.to_string();
        tokio::task::spawn_blocking(move || ops.write_record(&k, data))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<super::BoxPartSink<'a>, CloudHomeError> {
        // Clear any existing records before the first part lands.
        let ops = self.ops.clone();
        let k = key.to_string();
        tokio::task::spawn_blocking(move || delete_all_variants(&*ops, &k))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))??;
        Ok(Box::new(CloudKitPartSink {
            ops: self.ops.clone(),
            key: key.to_string(),
            index: 0,
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        CHUNK_SIZE as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let ops = self.ops.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            // Try single record first
            if ops.record_exists(&key)? {
                return ops.read_record(&key);
            }

            // Try chunked records
            let chunk_prefix = format!("{key}.part");
            let chunk_keys = ops.list_records(&chunk_prefix)?;
            if chunk_keys.is_empty() {
                return Err(CloudHomeError::NotFound(key));
            }

            // Parse part numbers up front; a key without a valid `.part{N}` suffix
            // would corrupt assembly order if treated as part 0.
            let mut numbered: Vec<(usize, String)> = chunk_keys
                .into_iter()
                .map(|k| {
                    let n = k
                        .rsplit_once(".part")
                        .and_then(|(_, suffix)| suffix.parse::<usize>().ok())
                        .ok_or_else(|| {
                            CloudHomeError::Storage(format!("chunk key {k:?} missing .part suffix"))
                        })?;
                    Ok::<_, CloudHomeError>((n, k))
                })
                .collect::<Result<_, _>>()?;
            numbered.sort_by_key(|(n, _)| *n);

            let mut result = Vec::new();
            for (_, chunk_key) in &numbered {
                let chunk = ops.read_record(chunk_key)?;
                result.extend_from_slice(&chunk);
            }
            Ok(result)
        })
        .await
        .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        if end <= start {
            return Ok(Vec::new());
        }

        let ops = self.ops.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let start = start as usize;
            let end = end as usize;

            // Try single record first
            if ops.record_exists(&key)? {
                let data = ops.read_record(&key)?;
                if end > data.len() {
                    return Err(CloudHomeError::Storage(format!(
                        "range {start}..{end} exceeds file size {}",
                        data.len()
                    )));
                }
                return Ok(data[start..end].to_vec());
            }

            // Chunked read: calculate which chunks overlap [start, end)
            let first_chunk = start / CHUNK_SIZE;
            let last_chunk = (end - 1) / CHUNK_SIZE;

            let mut result = Vec::with_capacity(end - start);
            for i in first_chunk..=last_chunk {
                let chunk_key = format!("{key}.part{i}");
                let chunk = ops.read_record(&chunk_key)?;

                let chunk_start = i * CHUNK_SIZE;
                let slice_start = if i == first_chunk {
                    start - chunk_start
                } else {
                    0
                };
                let slice_end = if i == last_chunk {
                    end - chunk_start
                } else {
                    chunk.len()
                };
                result.extend_from_slice(&chunk[slice_start..slice_end]);
            }
            Ok(result)
        })
        .await
        .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let ops = self.ops.clone();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let raw_keys = ops.list_records(&prefix)?;

            // Strip .partN suffixes and deduplicate
            let mut base_keys: Vec<String> = raw_keys
                .iter()
                .map(|k| strip_part_suffix(k).to_string())
                .collect();
            base_keys.sort();
            base_keys.dedup();
            Ok(base_keys)
        })
        .await
        .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || delete_all_variants(&*ops, &key))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let ops = self.ops.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            if ops.record_exists(&key)? {
                return Ok(true);
            }
            let chunk_prefix = format!("{key}.part");
            let chunks = ops.list_records(&chunk_prefix)?;
            Ok(!chunks.is_empty())
        })
        .await
        .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let ops = self.ops.clone();
        let member_id = member_id.to_string();
        tokio::task::spawn_blocking(move || {
            let share_url = ops.grant_access(&member_id)?;
            Ok(CloudHomeJoinInfo::CloudKit { share_url })
        })
        .await
        .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        let ops = self.ops.clone();
        let member_id = member_id.to_string();
        tokio::task::spawn_blocking(move || ops.revoke_access(&member_id))
            .await
            .map_err(|e| CloudHomeError::Storage(format!("spawn_blocking failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::BlobBody;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockCloudKitOps {
        store: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MockCloudKitOps {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            }
        }
    }

    impl CloudKitOps for MockCloudKitOps {
        fn write_record(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            self.store.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }

        fn read_record(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
        }

        fn list_records(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            let store = self.store.lock().unwrap();
            let mut keys: Vec<String> = store
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        fn delete_record(&self, key: &str) -> Result<(), CloudHomeError> {
            self.store.lock().unwrap().remove(key);
            Ok(())
        }

        fn record_exists(&self, key: &str) -> Result<bool, CloudHomeError> {
            Ok(self.store.lock().unwrap().contains_key(key))
        }

        fn grant_access(&self, email: &str) -> Result<String, CloudHomeError> {
            Ok(format!("https://www.icloud.com/share/{email}"))
        }

        fn revoke_access(&self, _user_record_id: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }

        fn accept_share(&self, _share_url: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }
    }

    fn make_cloud_home() -> CloudKitCloudHome {
        CloudKitCloudHome::new(Arc::new(MockCloudKitOps::new()))
    }

    /// A progress sink that discards its reports, for tests that only assert
    /// the stored bytes round-trip.
    fn no_progress() -> impl Fn(u64) + Send + Sync {
        |_| {}
    }

    #[tokio::test]
    async fn write_reports_progress_per_chunk_record() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let ch = make_cloud_home();
        // 25 MB spans three records (10 + 10 + 5) so progress fires three
        // times, the last equalling the total.
        let total = 25 * 1024 * 1024u64;
        let data: Vec<u8> = vec![0u8; total as usize];
        let last = Arc::new(AtomicU64::new(0));
        let ticks = Arc::new(AtomicU64::new(0));
        let last2 = last.clone();
        let ticks2 = ticks.clone();
        let sink = move |n: u64| {
            last2.store(n, Ordering::Relaxed);
            ticks2.fetch_add(1, Ordering::Relaxed);
        };
        ch.write("chunked.bin", BlobBody::from_bytes(data), &sink)
            .await
            .unwrap();
        assert_eq!(last.load(Ordering::Relaxed), total);
        assert_eq!(ticks.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_small_file_roundtrip() {
        let ch = make_cloud_home();
        let data = b"hello world".to_vec();
        ch.write(
            "small.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();
        let read = ch.read("small.bin").await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn test_large_file_roundtrip() {
        let ch = make_cloud_home();
        // 25MB of data -- spans 3 chunks (10 + 10 + 5)
        let data: Vec<u8> = (0..25 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        ch.write(
            "large.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();
        let read = ch.read("large.bin").await.unwrap();
        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn test_read_range_single() {
        let ch = make_cloud_home();
        ch.write(
            "range.bin",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        let slice = ch.read_range("range.bin", 3, 7).await.unwrap();
        assert_eq!(slice, b"3456");
    }

    #[tokio::test]
    async fn test_read_range_chunked() {
        let ch = make_cloud_home();
        // Create data that spans 2 chunks: 15MB
        let data: Vec<u8> = (0..15 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        ch.write(
            "big.bin",
            BlobBody::from_bytes(data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        // Read a range that crosses the chunk boundary (last byte of chunk 0, first byte of chunk 1)
        let boundary = CHUNK_SIZE;
        let start = (boundary - 2) as u64;
        let end = (boundary + 3) as u64;
        let slice = ch.read_range("big.bin", start, end).await.unwrap();
        assert_eq!(slice.len(), 5);
        assert_eq!(slice, &data[start as usize..end as usize]);
    }

    #[tokio::test]
    async fn test_list_deduplicates_chunks() {
        let ch = make_cloud_home();
        // Write a chunked file
        let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write(
            "files/album.flac",
            BlobBody::from_bytes(data),
            &no_progress(),
        )
        .await
        .unwrap();

        // Also write a small file
        ch.write(
            "files/cover.jpg",
            BlobBody::from_bytes(b"img".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        let keys = ch.list("files/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"files/album.flac".to_string()));
        assert!(keys.contains(&"files/cover.jpg".to_string()));
    }

    #[tokio::test]
    async fn test_delete_removes_all_chunks() {
        let ch = make_cloud_home();
        let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("to-delete.bin", BlobBody::from_bytes(data), &no_progress())
            .await
            .unwrap();

        assert!(ch.exists("to-delete.bin").await.unwrap());

        ch.delete("to-delete.bin").await.unwrap();

        assert!(!ch.exists("to-delete.bin").await.unwrap());

        // Verify the underlying ops store is empty of related keys
        let ops = &ch.ops;
        let keys = ops.list_records("to-delete.bin").unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_overwrite_chunked_with_single() {
        let ch = make_cloud_home();
        // Write large file (chunked)
        let large_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
        ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
            .await
            .unwrap();

        // Overwrite with small file (single record)
        let small_data = b"small".to_vec();
        ch.write(
            "file.bin",
            BlobBody::from_bytes(small_data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read("file.bin").await.unwrap();
        assert_eq!(read, small_data);

        // Verify no chunk records remain
        let chunks = ch.ops.list_records("file.bin.part").unwrap();
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn test_overwrite_single_with_chunked() {
        let ch = make_cloud_home();
        // Write small file
        ch.write(
            "file.bin",
            BlobBody::from_bytes(b"small".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        // Overwrite with large file (chunked)
        let large_data: Vec<u8> = vec![1u8; 25 * 1024 * 1024];
        ch.write(
            "file.bin",
            BlobBody::from_bytes(large_data.clone()),
            &no_progress(),
        )
        .await
        .unwrap();

        let read = ch.read("file.bin").await.unwrap();
        assert_eq!(read, large_data);

        // Verify no single record remains (the base key should not exist)
        assert!(!ch.ops.record_exists("file.bin").unwrap());
    }

    #[tokio::test]
    async fn test_exists() {
        let ch = make_cloud_home();

        assert!(!ch.exists("nope.bin").await.unwrap());

        ch.write(
            "yep.bin",
            BlobBody::from_bytes(b"data".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        assert!(ch.exists("yep.bin").await.unwrap());

        // Chunked file
        let data: Vec<u8> = vec![0u8; 15 * 1024 * 1024];
        ch.write("chunked.bin", BlobBody::from_bytes(data), &no_progress())
            .await
            .unwrap();
        assert!(ch.exists("chunked.bin").await.unwrap());
    }

    #[tokio::test]
    async fn test_read_range_empty_when_end_leq_start() {
        let ch = make_cloud_home();
        ch.write(
            "range.bin",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        // end == start returns empty
        let slice = ch.read_range("range.bin", 3, 3).await.unwrap();
        assert!(slice.is_empty());

        // end < start returns empty
        let slice = ch.read_range("range.bin", 5, 2).await.unwrap();
        assert!(slice.is_empty());

        // end == 0 returns empty (the underflow case)
        let slice = ch.read_range("range.bin", 0, 0).await.unwrap();
        assert!(slice.is_empty());
    }

    #[test]
    fn test_strip_part_suffix() {
        assert_eq!(strip_part_suffix("file.bin.part0"), "file.bin");
        assert_eq!(strip_part_suffix("file.bin.part123"), "file.bin");
        assert_eq!(strip_part_suffix("file.bin"), "file.bin");
        assert_eq!(strip_part_suffix("file.partition"), "file.partition");
        assert_eq!(strip_part_suffix("file.part"), "file.part"); // no digits after .part
    }
}
