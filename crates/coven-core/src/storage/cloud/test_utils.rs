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
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, CloudObjectState,
    CloudObjectVersion, ConditionalDelete, PartSink,
};

/// In-memory CloudHome backed by a HashMap. `Clone` shares one backing store, so
/// clones act as separate devices reading and writing the same cloud bucket, and
/// a test can keep its own handle for direct at-rest assertions while each device
/// owns a `Box<dyn CloudHome>` clone.
#[derive(Clone)]
pub struct InMemoryCloudHome {
    writes: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    deletes: Arc<Mutex<Vec<String>>>,
}

impl InMemoryCloudHome {
    pub fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(HashMap::new())),
            deletes: Arc::new(Mutex::new(Vec::new())),
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
        self.writes.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
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

    async fn object_state(&self, key: &str) -> Result<CloudObjectState, CloudHomeError> {
        let writes = self.writes.lock().unwrap();
        match writes.get(key) {
            Some(data) => Ok(CloudObjectState::Present(content_version(data))),
            None => Ok(CloudObjectState::Absent),
        }
    }

    async fn delete_if_version(
        &self,
        key: &str,
        version: &CloudObjectVersion,
    ) -> Result<ConditionalDelete, CloudHomeError> {
        let mut writes = self.writes.lock().unwrap();
        let Some(data) = writes.get(key) else {
            return Ok(ConditionalDelete::NotFound);
        };
        if content_version(data) != *version {
            return Ok(ConditionalDelete::Changed);
        }
        writes.remove(key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(ConditionalDelete::Deleted)
    }

    async fn grant_access(
        &self,
        _grant: super::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Err(CloudHomeError::Storage(
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

fn content_version(data: &[u8]) -> CloudObjectVersion {
    CloudObjectVersion::new(hex::encode(Sha256::digest(data)))
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
}
