//! In-process CloudHome implementation for tests. Records every write keyed
//! by cloud_key so tests can read back exactly what landed, and serves reads
//! from the same map — enough to simulate two devices sharing a cloud bucket.
//!
//! Available under `#[cfg(test)]` in coven itself and to downstream crates
//! that enable the `test-utils` feature.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

/// In-memory CloudHome backed by a HashMap. Thread-safe; cheap to share
/// between simulated devices via `Arc`.
pub struct InMemoryCloudHome {
    writes: Mutex<HashMap<String, Vec<u8>>>,
    deletes: Mutex<Vec<String>>,
}

impl InMemoryCloudHome {
    pub fn new() -> Self {
        Self {
            writes: Mutex::new(HashMap::new()),
            deletes: Mutex::new(Vec::new()),
        }
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for InMemoryCloudHome {
    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        // Chunk the report so tests exercise the same incremental-progress path
        // the real backends drive, then store the whole buffer at once.
        let total = data.len() as u64;
        let mut sent = 0u64;
        for chunk in data.chunks(super::PROGRESS_CHUNK_SIZE) {
            sent += chunk.len() as u64;
            progress(sent);
        }
        // An empty write reports nothing above; signal completion so a
        // zero-byte blob still reaches 100%.
        if total == 0 {
            progress(0);
        }
        self.writes.lock().unwrap().insert(key.to_string(), data);
        Ok(())
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
        let data = self.read(key).await?;
        let s = start as usize;
        let e = (end as usize).min(data.len());
        if s > data.len() {
            return Err(CloudHomeError::NotFound(format!("range past end of {key}")));
        }
        Ok(data[s..e].to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        Ok(self
            .writes
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.writes.lock().unwrap().remove(key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        Ok(self.writes.lock().unwrap().contains_key(key))
    }

    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Err(CloudHomeError::Storage(
            "InMemoryCloudHome does not grant access".into(),
        ))
    }

    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

/// A `CloudHome` handle that shares one backing [`InMemoryCloudHome`]. Cloning it
/// yields another handle onto the same store, so two `CloudSyncStorage` instances
/// built over clones model two devices reading and writing one cloud bucket. The
/// inner `Arc` also lets a test keep its own handle for direct at-rest assertions
/// while each device owns a `Box<dyn CloudHome>` clone.
#[derive(Clone, Default)]
pub struct SharedInMemoryCloudHome(Arc<InMemoryCloudHome>);

impl SharedInMemoryCloudHome {
    pub fn new() -> Self {
        Self(Arc::new(InMemoryCloudHome::new()))
    }

    /// Snapshot of the bytes at `key` in the shared store, or `None` if absent.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0.get(key)
    }

    /// Number of objects in the shared store.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the shared store is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for SharedInMemoryCloudHome {
    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        self.0.write(key, data, progress).await
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.0.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.0.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.0.exists(key).await
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        self.0.grant_access(member_id).await
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        self.0.revoke_access(member_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// A progress sink that does nothing — for tests that don't assert on
    /// progress reporting.
    fn no_progress() -> impl Fn(u64) + Send + Sync {
        |_| {}
    }

    #[tokio::test]
    async fn write_then_read_roundtrips() {
        let h = InMemoryCloudHome::new();
        h.write("foo", b"hello".to_vec(), &no_progress())
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
        h.write("big", vec![0u8; len], &sink).await.unwrap();
        assert_eq!(last.load(Ordering::Relaxed), len as u64);
        assert_eq!(ticks.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn read_range_returns_a_slice() {
        let h = InMemoryCloudHome::new();
        h.write("k", b"0123456789".to_vec(), &no_progress())
            .await
            .unwrap();
        assert_eq!(h.read_range("k", 2, 5).await.unwrap(), b"234");
    }

    #[tokio::test]
    async fn list_filters_by_prefix() {
        let h = InMemoryCloudHome::new();
        h.write("a/x", vec![1], &no_progress()).await.unwrap();
        h.write("a/y", vec![2], &no_progress()).await.unwrap();
        h.write("b/x", vec![3], &no_progress()).await.unwrap();
        let mut got = h.list("a/").await.unwrap();
        got.sort();
        assert_eq!(got, vec!["a/x".to_string(), "a/y".to_string()]);
    }

    #[tokio::test]
    async fn delete_removes_and_records() {
        let h = InMemoryCloudHome::new();
        h.write("k", vec![1], &no_progress()).await.unwrap();
        h.delete("k").await.unwrap();
        assert!(matches!(
            h.read("k").await,
            Err(CloudHomeError::NotFound(_))
        ));
        assert_eq!(h.deletes_seen(), vec!["k".to_string()]);
    }
}
