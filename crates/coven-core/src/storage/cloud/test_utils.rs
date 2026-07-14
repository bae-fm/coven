//! In-process CloudHome implementation for tests. Records every write keyed
//! by cloud_key so tests can read back exactly what landed, and serves reads
//! from the same map — enough to simulate two devices sharing a cloud bucket.
//!
//! Available under `#[cfg(test)]` in coven itself and to downstream crates
//! that enable the `test-utils` feature.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::{BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, PartSink};

/// In-memory CloudHome backed by a HashMap. `Clone` shares one backing store, so
/// clones act as separate devices reading and writing the same cloud bucket, and
/// a test can keep its own handle for direct at-rest assertions while each device
/// owns a `Box<dyn CloudHome>` clone.
///
/// Beyond the happy path it carries fault-injection knobs
/// ([`arm_write_failures`](Self::arm_write_failures),
/// [`fail_next_range_reads`](Self::fail_next_range_reads),
/// [`remove`](Self::remove)) so a host test can drive upload-failure,
/// read-retry, and missing-blob paths without a bespoke `CloudHome` impl. The
/// arming state is shared across clones, like the backing store.
#[derive(Clone)]
pub struct InMemoryCloudHome {
    writes: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    deletes: Arc<Mutex<Vec<String>>>,
    fail_writes: Arc<AtomicBool>,
    fail_next_range_reads: Arc<AtomicUsize>,
    sort_listings: Arc<AtomicBool>,
}

impl InMemoryCloudHome {
    pub fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(HashMap::new())),
            deletes: Arc::new(Mutex::new(Vec::new())),
            fail_writes: Arc::new(AtomicBool::new(false)),
            fail_next_range_reads: Arc::new(AtomicUsize::new(0)),
            sort_listings: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Return `list` results in sorted key order instead of the backing map's
    /// arbitrary order. A real bucket LIST has no defined order, so the pull's
    /// cross-device apply order is arbitrary; a test that needs a fixed order (to
    /// reproduce an order-dependent bug deterministically) arms this and picks the
    /// order through its device ids.
    pub fn sort_listings(&self) {
        self.sort_listings.store(true, Ordering::SeqCst);
    }

    /// Arm every subsequent write (`put_object` and `open_multipart`) to fail
    /// with a retryable transport error. A test can let a home's setup writes
    /// land and then arm this before driving the path whose uploads must fail;
    /// it stays armed for the store's lifetime.
    pub fn arm_write_failures(&self) {
        self.fail_writes.store(true, Ordering::SeqCst);
    }

    /// Make the next `n` `read_range` calls fail with a retryable transport
    /// error before any serves bytes, to exercise a caller's read-retry path.
    /// Each failed call consumes one; once `n` are spent, ranges serve
    /// normally.
    pub fn fail_next_range_reads(&self, n: usize) {
        self.fail_next_range_reads.store(n, Ordering::SeqCst);
    }

    /// Drop `key`'s bytes out of band — as if the object vanished from the
    /// bucket on its own, without a `delete` (which `deletes_seen` would
    /// record). Drives missing-blob read failures.
    pub fn remove(&self, key: &str) {
        self.writes.lock().unwrap().remove(key);
    }

    /// Snapshot of every key currently in the cloud. Useful for assertions
    /// that don't want to hold the lock across an await.
    pub fn keys(&self) -> Vec<String> {
        self.writes.lock().unwrap().keys().cloned().collect()
    }

    /// Snapshot of the bytes at `key`, or `None` if absent. Cloned so the
    /// caller can hold the result across `await` points without retaining
    /// the internal lock.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.writes.lock().unwrap().get(key).cloned()
    }

    /// Number of objects stored. Cheap snapshot.
    pub fn len(&self) -> usize {
        self.writes.lock().unwrap().len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.writes.lock().unwrap().is_empty()
    }

    /// Snapshot of every delete that's been requested, in arrival order.
    pub fn deletes_seen(&self) -> Vec<String> {
        self.deletes.lock().unwrap().clone()
    }
}

impl Default for InMemoryCloudHome {
    fn default() -> Self {
        Self::new()
    }
}

/// A [`PartSink`] for the in-memory backend: accumulate the streamed parts in
/// order and store the assembled object on `finish`, so a multipart upload
/// round-trips exactly like a single `put_object`.
struct InMemoryPartSink {
    writes: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    key: String,
    buf: Vec<u8>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PartSink for InMemoryPartSink {
    fn part_size(&self) -> usize {
        super::PROGRESS_CHUNK_SIZE
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        self.buf.extend_from_slice(&part);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        self.writes.lock().unwrap().insert(self.key, self.buf);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for InMemoryCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed write failure".into(),
            ));
        }
        self.writes.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        // Gate multipart too, so `arm_write_failures` fails a write whatever its
        // size — `write_blob` routes blobs above `multipart_threshold` here.
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed write failure".into(),
            ));
        }
        Ok(Box::new(InMemoryPartSink {
            writes: self.writes.clone(),
            key: key.to_string(),
            buf: Vec::new(),
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        // A small threshold so tests exercise the multipart driver path; the part
        // size matches so a multi-part blob ticks progress several times.
        super::PROGRESS_CHUNK_SIZE as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.writes
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        // An armed range read fails before touching the store. `checked_sub`
        // returns `None` at zero, so `fetch_update` only succeeds (and errors)
        // while the countdown is positive.
        if self
            .fail_next_range_reads
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed range-read failure".into(),
            ));
        }
        let data = self.read(key).await?;
        let s = start as usize;
        let e = (end as usize).min(data.len());
        if s > data.len() {
            return Err(CloudHomeError::NotFound(format!("range past end of {key}")));
        }
        Ok(data[s..e].to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let mut keys: Vec<String> = self
            .writes
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        if self.sort_listings.load(Ordering::SeqCst) {
            keys.sort();
        }
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.writes.lock().unwrap().remove(key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        Ok(self.writes.lock().unwrap().contains_key(key))
    }

    async fn grant_access(
        &self,
        _grant: super::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Err(CloudHomeError::Transport(
            "InMemoryCloudHome does not grant access".into(),
        ))
    }

    async fn revoke_access(
        &self,
        _revoke: super::CloudAccessRevoke,
    ) -> Result<super::RevokeOutcome, CloudHomeError> {
        Ok(super::RevokeOutcome::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::{no_progress, BlobBody};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn write_then_read_roundtrips() {
        let h = InMemoryCloudHome::new();
        h.write(
            "foo",
            BlobBody::from_bytes(b"hello".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        assert_eq!(h.read("foo").await.unwrap(), b"hello");
        assert!(h.exists("foo").await.unwrap());
        assert!(!h.exists("bar").await.unwrap());
    }

    #[tokio::test]
    async fn write_reports_progress_in_chunks_reaching_the_total() {
        let h = InMemoryCloudHome::new();
        // Two-and-a-bit chunks so progress fires more than once and the final
        // value equals the total.
        let len = super::super::PROGRESS_CHUNK_SIZE * 2 + 7;
        let last = Arc::new(AtomicU64::new(0));
        let ticks = Arc::new(AtomicU64::new(0));
        let last2 = last.clone();
        let ticks2 = ticks.clone();
        let sink = move |n: u64| {
            last2.store(n, Ordering::Relaxed);
            ticks2.fetch_add(1, Ordering::Relaxed);
        };
        h.write("big", BlobBody::from_bytes(vec![0u8; len]), &sink)
            .await
            .unwrap();
        assert_eq!(last.load(Ordering::Relaxed), len as u64);
        assert_eq!(ticks.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn read_range_returns_a_slice() {
        let h = InMemoryCloudHome::new();
        h.write(
            "k",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        assert_eq!(h.read_range("k", 2, 5).await.unwrap(), b"234");
    }

    #[tokio::test]
    async fn list_filters_by_prefix() {
        let h = InMemoryCloudHome::new();
        h.write("a/x", BlobBody::from_bytes(vec![1]), &no_progress())
            .await
            .unwrap();
        h.write("a/y", BlobBody::from_bytes(vec![2]), &no_progress())
            .await
            .unwrap();
        h.write("b/x", BlobBody::from_bytes(vec![3]), &no_progress())
            .await
            .unwrap();
        let mut got = h.list("a/").await.unwrap();
        got.sort();
        assert_eq!(got, vec!["a/x".to_string(), "a/y".to_string()]);
    }

    #[tokio::test]
    async fn delete_removes_and_records() {
        let h = InMemoryCloudHome::new();
        h.write("k", BlobBody::from_bytes(vec![1]), &no_progress())
            .await
            .unwrap();
        h.delete("k").await.unwrap();
        assert!(matches!(
            h.read("k").await,
            Err(CloudHomeError::NotFound(_))
        ));
        assert_eq!(h.deletes_seen(), vec!["k".to_string()]);
    }

    #[tokio::test]
    async fn arm_write_failures_fails_writes_after_arming() {
        let h = InMemoryCloudHome::new();
        // Writes land before arming.
        h.write("before", BlobBody::from_bytes(vec![1]), &no_progress())
            .await
            .unwrap();

        h.arm_write_failures();
        let err = h
            .write("after", BlobBody::from_bytes(vec![2]), &no_progress())
            .await
            .unwrap_err();
        assert!(matches!(err, CloudHomeError::Transport(_)));
        assert!(err.is_retryable());
        // Nothing was stored for the failed write, and the earlier one survives.
        assert!(h.get("after").is_none());
        assert_eq!(h.get("before"), Some(vec![1]));
    }

    #[tokio::test]
    async fn fail_next_range_reads_fails_the_next_n_then_recovers() {
        let h = InMemoryCloudHome::new();
        h.write(
            "k",
            BlobBody::from_bytes(b"0123456789".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        h.fail_next_range_reads(2);
        assert!(matches!(
            h.read_range("k", 0, 4).await,
            Err(CloudHomeError::Transport(_))
        ));
        assert!(matches!(
            h.read_range("k", 0, 4).await,
            Err(CloudHomeError::Transport(_))
        ));
        // The third serves real bytes — the countdown is spent.
        assert_eq!(h.read_range("k", 0, 4).await.unwrap(), b"0123");
    }

    #[tokio::test]
    async fn remove_drops_a_key_out_of_band() {
        let h = InMemoryCloudHome::new();
        h.write("k", BlobBody::from_bytes(vec![1]), &no_progress())
            .await
            .unwrap();

        h.remove("k");
        assert!(matches!(
            h.read("k").await,
            Err(CloudHomeError::NotFound(_))
        ));
        // Out-of-band removal is not a delete, so it leaves no delete record.
        assert!(h.deletes_seen().is_empty());
    }
}
