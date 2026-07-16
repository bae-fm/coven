//! In-process CloudHome implementation for tests. Records every write keyed
//! by cloud_key so tests can read back exactly what landed, and serves reads
//! from the same map — enough to simulate two devices sharing a cloud bucket.
//!
//! Available under `#[cfg(test)]` in coven itself and to downstream crates
//! that enable the `test-utils` feature.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::{
    AppendedListing, AppendedObject, BlobBody, BoxPartSink, CloudHeadCreateError,
    CloudHeadReplaceError, CloudHeadStorage, CloudHeadVersion, CloudHome, CloudHomeError,
    CloudVersionedHead, ListingCoverage, PartSink, UploadProgress,
};

#[derive(Clone)]
struct MemoryAppendedObject {
    locator: AppendedObject,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct AppendPause {
    call: usize,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct ListingPause {
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

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
    head_versions: Arc<Mutex<HashMap<String, u64>>>,
    appended: Arc<Mutex<Vec<MemoryAppendedObject>>>,
    next_appended_id: Arc<AtomicU64>,
    deletes: Arc<Mutex<Vec<String>>>,
    fail_writes: Arc<AtomicBool>,
    fail_next_range_reads: Arc<AtomicUsize>,
    sort_listings: Arc<AtomicBool>,
    append_count: Arc<AtomicUsize>,
    fail_append_before: Arc<AtomicUsize>,
    fail_append_after: Arc<AtomicUsize>,
    corrupt_append_readback: Arc<AtomicUsize>,
    append_pause: Arc<Mutex<Option<AppendPause>>>,
    appended_listing_pause: Arc<Mutex<Option<ListingPause>>>,
    appended_list_count: Arc<AtomicUsize>,
    appended_full_read_count: Arc<AtomicUsize>,
    appended_stream_read_count: Arc<AtomicUsize>,
    listing_coverage: Arc<Mutex<ListingCoverage>>,
    appended_delete_count: Arc<AtomicUsize>,
    fail_appended_delete_on: Arc<AtomicUsize>,
    fail_head_cleanup: Arc<AtomicBool>,
    head_mutation_count: Arc<AtomicUsize>,
    fail_head_after_mutation: Arc<AtomicBool>,
    head_after_mutation_override: Arc<Mutex<Option<Vec<u8>>>>,
}

impl InMemoryCloudHome {
    pub fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(HashMap::new())),
            head_versions: Arc::new(Mutex::new(HashMap::new())),
            appended: Arc::new(Mutex::new(Vec::new())),
            next_appended_id: Arc::new(AtomicU64::new(0)),
            deletes: Arc::new(Mutex::new(Vec::new())),
            fail_writes: Arc::new(AtomicBool::new(false)),
            fail_next_range_reads: Arc::new(AtomicUsize::new(0)),
            sort_listings: Arc::new(AtomicBool::new(false)),
            append_count: Arc::new(AtomicUsize::new(0)),
            fail_append_before: Arc::new(AtomicUsize::new(0)),
            fail_append_after: Arc::new(AtomicUsize::new(0)),
            corrupt_append_readback: Arc::new(AtomicUsize::new(0)),
            append_pause: Arc::new(Mutex::new(None)),
            appended_listing_pause: Arc::new(Mutex::new(None)),
            appended_list_count: Arc::new(AtomicUsize::new(0)),
            appended_full_read_count: Arc::new(AtomicUsize::new(0)),
            appended_stream_read_count: Arc::new(AtomicUsize::new(0)),
            listing_coverage: Arc::new(Mutex::new(ListingCoverage::CompleteAtScan)),
            appended_delete_count: Arc::new(AtomicUsize::new(0)),
            fail_appended_delete_on: Arc::new(AtomicUsize::new(0)),
            fail_head_cleanup: Arc::new(AtomicBool::new(false)),
            head_mutation_count: Arc::new(AtomicUsize::new(0)),
            fail_head_after_mutation: Arc::new(AtomicBool::new(false)),
            head_after_mutation_override: Arc::new(Mutex::new(None)),
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

    /// Reset the immutable-append counter and fail before the selected call
    /// stores a physical copy.
    pub fn fail_append_before_call(&self, call: usize) {
        assert!(call > 0, "append call numbers are 1-based");
        self.append_count.store(0, Ordering::SeqCst);
        self.fail_append_before.store(call, Ordering::SeqCst);
    }

    /// Reset the immutable-append counter and fail after the selected call has
    /// stored its physical copy, modeling an ambiguous provider response.
    pub fn fail_append_after_call(&self, call: usize) {
        assert!(call > 0, "append call numbers are 1-based");
        self.append_count.store(0, Ordering::SeqCst);
        self.fail_append_after.store(call, Ordering::SeqCst);
    }

    /// Reset the immutable-append counter and replace the selected physical
    /// copy before its immediate verification read.
    pub fn corrupt_append_readback_on_call(&self, call: usize) {
        assert!(call > 0, "append call numbers are 1-based");
        self.append_count.store(0, Ordering::SeqCst);
        self.corrupt_append_readback.store(call, Ordering::SeqCst);
    }

    /// Pause after the selected immutable append is physically visible. The
    /// returned notifications report that visibility and release the call.
    pub fn pause_after_append_call(
        &self,
        call: usize,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        assert!(call > 0, "append call numbers are 1-based");
        self.append_count.store(0, Ordering::SeqCst);
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.append_pause.lock().unwrap() = Some(AppendPause {
            call,
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    /// Pause the next immutable-object listing before it reads provider state.
    pub fn pause_next_appended_listing(
        &self,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.appended_listing_pause.lock().unwrap() = Some(ListingPause {
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    pub fn append_count(&self) -> usize {
        self.append_count.load(Ordering::SeqCst)
    }

    pub fn appended_list_count(&self) -> usize {
        self.appended_list_count.load(Ordering::SeqCst)
    }

    pub fn appended_full_read_count(&self) -> usize {
        self.appended_full_read_count.load(Ordering::SeqCst)
    }

    pub fn appended_stream_read_count(&self) -> usize {
        self.appended_stream_read_count.load(Ordering::SeqCst)
    }

    pub fn appended_delete_count(&self) -> usize {
        self.appended_delete_count.load(Ordering::SeqCst)
    }

    pub fn set_listing_coverage(&self, coverage: ListingCoverage) {
        *self.listing_coverage.lock().unwrap() = coverage;
    }

    pub fn fail_appended_delete_on_call(&self, call: usize) {
        assert!(call > 0, "append-delete call numbers are 1-based");
        self.appended_delete_count.store(0, Ordering::SeqCst);
        self.fail_appended_delete_on.store(call, Ordering::SeqCst);
    }

    pub fn fail_coordination_probe_cleanup(&self) {
        self.fail_head_cleanup.store(true, Ordering::SeqCst);
    }

    pub fn head_mutation_count(&self) -> usize {
        self.head_mutation_count.load(Ordering::SeqCst)
    }

    pub fn fail_next_head_mutation_after_visibility(&self) {
        self.fail_head_after_mutation.store(true, Ordering::SeqCst);
    }

    pub fn replace_after_next_head_mutation(&self, replacement: Vec<u8>) {
        *self.head_after_mutation_override.lock().unwrap() = Some(replacement);
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

    /// Snapshot of every immutable physical-copy key currently in the cloud.
    pub fn appended_keys(&self) -> Vec<String> {
        self.appended
            .lock()
            .unwrap()
            .iter()
            .map(|candidate| candidate.locator.logical_key().to_string())
            .collect()
    }

    /// Snapshot of one immutable physical copy's stored bytes.
    pub fn get_appended(&self, logical_key: &str) -> Option<Vec<u8>> {
        self.appended
            .lock()
            .unwrap()
            .iter()
            .find(|candidate| candidate.locator.logical_key() == logical_key)
            .map(|candidate| candidate.bytes.clone())
    }

    fn appended_bytes(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        if let Some(bytes) = self
            .appended
            .lock()
            .unwrap()
            .iter()
            .find(|candidate| candidate.locator == *object)
            .map(|candidate| candidate.bytes.clone())
        {
            return Ok(bytes);
        }
        self.writes
            .lock()
            .unwrap()
            .get(object.logical_key())
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(object.opaque_provider_id().to_string()))
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

    /// Insert a physical append candidate with caller-selected bytes. This is
    /// the collision/fork test hook; it does not pass through protocol parsing.
    pub fn insert_appended_candidate(&self, logical_key: &str, bytes: Vec<u8>) -> AppendedObject {
        let id = self.next_appended_id.fetch_add(1, Ordering::SeqCst);
        let locator = AppendedObject::from_provider(
            logical_key.to_string(),
            format!("in-memory-append-{id}"),
        );
        self.appended.lock().unwrap().push(MemoryAppendedObject {
            locator: locator.clone(),
            bytes,
        });
        locator
    }

    /// Remove exactly one appended physical object without recording a protocol
    /// delete, simulating provider-side disappearance.
    pub fn remove_appended_candidate(&self, locator: &AppendedObject) {
        self.appended
            .lock()
            .unwrap()
            .retain(|candidate| candidate.locator != *locator);
    }

    /// Replace the bytes at one exact physical locator without changing its
    /// logical key or provider id.
    pub fn replace_appended_candidate(&self, locator: &AppendedObject, bytes: Vec<u8>) {
        let mut appended = self.appended.lock().unwrap();
        let candidate = appended
            .iter_mut()
            .find(|candidate| candidate.locator == *locator)
            .expect("appended locator exists");
        candidate.bytes = bytes;
    }
}

impl Default for InMemoryCloudHome {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CloudHeadStorage for InMemoryCloudHome {
    async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
        let writes = self.writes.lock().unwrap();
        let versions = self.head_versions.lock().unwrap();
        let bytes = writes
            .get(key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        let version = versions.get(key).copied().ok_or_else(|| {
            CloudHomeError::Configuration(format!(
                "coordination head {key:?} has bytes without a version"
            ))
        })?;
        Ok(CloudVersionedHead {
            bytes,
            version: CloudHeadVersion::from_provider(version.to_string())?,
        })
    }

    async fn create_head(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        let mut writes = self.writes.lock().unwrap();
        let mut versions = self.head_versions.lock().unwrap();
        if writes.contains_key(key) {
            return Err(CloudHeadCreateError::AlreadyExists);
        }
        let version = 1_u64;
        writes.insert(key.to_string(), bytes.clone());
        versions.insert(key.to_string(), version);
        self.head_mutation_count.fetch_add(1, Ordering::SeqCst);
        if let Some(replacement) = self.head_after_mutation_override.lock().unwrap().take() {
            writes.insert(key.to_string(), replacement);
            versions.insert(key.to_string(), version + 1);
            self.head_mutation_count.fetch_add(1, Ordering::SeqCst);
            return Err(CloudHeadCreateError::Storage(CloudHomeError::Transport(
                "injected competing head after visible create".to_string(),
            )));
        }
        if self.fail_head_after_mutation.swap(false, Ordering::SeqCst) {
            return Err(CloudHeadCreateError::Storage(CloudHomeError::Transport(
                "injected lost response after visible create".to_string(),
            )));
        }
        Ok(CloudVersionedHead {
            bytes,
            version: CloudHeadVersion::from_provider(version.to_string())?,
        })
    }

    async fn replace_head(
        &self,
        key: &str,
        expected: &CloudHeadVersion,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
        let mut writes = self.writes.lock().unwrap();
        let mut versions = self.head_versions.lock().unwrap();
        let current = versions
            .get(key)
            .copied()
            .ok_or(CloudHeadReplaceError::VersionMismatch)?;
        if current.to_string() != expected.as_provider() || !writes.contains_key(key) {
            return Err(CloudHeadReplaceError::VersionMismatch);
        }
        let version = current.checked_add(1).ok_or_else(|| {
            CloudHeadReplaceError::Storage(CloudHomeError::Configuration(
                "coordination head version exhausted".to_string(),
            ))
        })?;
        writes.insert(key.to_string(), bytes.clone());
        versions.insert(key.to_string(), version);
        self.head_mutation_count.fetch_add(1, Ordering::SeqCst);
        if let Some(replacement) = self.head_after_mutation_override.lock().unwrap().take() {
            writes.insert(key.to_string(), replacement);
            versions.insert(key.to_string(), version + 1);
            self.head_mutation_count.fetch_add(1, Ordering::SeqCst);
            return Err(CloudHeadReplaceError::Storage(CloudHomeError::Transport(
                "injected competing head after visible replace".to_string(),
            )));
        }
        if self.fail_head_after_mutation.swap(false, Ordering::SeqCst) {
            return Err(CloudHeadReplaceError::Storage(CloudHomeError::Transport(
                "injected lost response after visible replace".to_string(),
            )));
        }
        Ok(CloudVersionedHead {
            bytes,
            version: CloudHeadVersion::from_provider(version.to_string())?,
        })
    }

    async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
        if self.fail_head_cleanup.swap(false, Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed coordination cleanup failure".to_string(),
            ));
        }
        let mut writes = self.writes.lock().unwrap();
        let mut versions = self.head_versions.lock().unwrap();
        writes.remove(key);
        versions.remove(key);
        self.deletes.lock().unwrap().push(key.to_string());
        Ok(())
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

#[async_trait]
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

#[async_trait]
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

    async fn append_object(
        &self,
        full_logical_key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed write failure".into(),
            ));
        }
        let call = self.append_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_append_before.load(Ordering::SeqCst) == call {
            self.fail_append_before.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced failure before append call {call}"
            )));
        }
        let bytes = body.collect().await?;
        progress(bytes.len() as u64);
        let appended = self.insert_appended_candidate(full_logical_key, bytes);
        if self.corrupt_append_readback.load(Ordering::SeqCst) == call {
            self.corrupt_append_readback.store(0, Ordering::SeqCst);
            self.replace_appended_candidate(&appended, b"corrupt readback".to_vec());
        }
        let pause = self
            .append_pause
            .lock()
            .unwrap()
            .clone()
            .filter(|pause| pause.call == call);
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.release.notified().await;
            self.append_pause.lock().unwrap().take();
        }
        if self.fail_append_after.load(Ordering::SeqCst) == call {
            self.fail_append_after.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced failure after append call {call}"
            )));
        }
        Ok(appended)
    }

    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        self.appended_list_count.fetch_add(1, Ordering::SeqCst);
        let pause = self.appended_listing_pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.release.notified().await;
            self.appended_listing_pause.lock().unwrap().take();
        }
        let mut objects: Vec<AppendedObject> = self
            .appended
            .lock()
            .unwrap()
            .iter()
            .filter(|candidate| candidate.locator.logical_key().starts_with(prefix))
            .map(|candidate| candidate.locator.clone())
            .collect();
        objects.extend(
            self.writes
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .map(|key| AppendedObject::from_provider(key.clone(), key.clone())),
        );
        if self.sort_listings.load(Ordering::SeqCst) {
            objects.sort_by(|left, right| {
                left.logical_key()
                    .cmp(right.logical_key())
                    .then_with(|| left.opaque_provider_id().cmp(right.opaque_provider_id()))
            });
        }
        Ok(AppendedListing {
            objects,
            coverage: *self.listing_coverage.lock().unwrap(),
        })
    }

    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        self.appended_full_read_count.fetch_add(1, Ordering::SeqCst);
        self.appended_bytes(object)
    }

    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        self.appended_stream_read_count
            .fetch_add(1, Ordering::SeqCst);
        let bytes = self.appended_bytes(object)?;
        let stream = futures_util::stream::once(async move { Ok(bytes::Bytes::from(bytes)) });
        super::write_cloud_object_stream(destination, Box::pin(stream)).await?;
        Ok(())
    }

    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        let call = self.appended_delete_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_appended_delete_on.load(Ordering::SeqCst) == call {
            self.fail_appended_delete_on.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced appended delete failure on call {call}"
            )));
        }
        let mut appended = self.appended.lock().unwrap();
        let before = appended.len();
        appended.retain(|candidate| candidate.locator != *object);
        if appended.len() == before {
            return Err(CloudHomeError::NotFound(
                object.opaque_provider_id().to_string(),
            ));
        }
        self.deletes
            .lock()
            .unwrap()
            .push(object.logical_key().to_string());
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

    async fn set_access(
        &self,
        desired: super::CloudAccessState,
    ) -> Result<super::CloudAccessOutcome, CloudHomeError> {
        match desired {
            super::CloudAccessState::Present { .. } => Err(CloudHomeError::Transport(
                "InMemoryCloudHome does not grant access".into(),
            )),
            super::CloudAccessState::Absent { .. } => Ok(super::CloudAccessOutcome::Absent(
                super::RevokeOutcome::Unsupported,
            )),
        }
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

    #[tokio::test]
    async fn append_harness_pauses_after_visibility_and_can_inspect_replace_and_remove() {
        let h = InMemoryCloudHome::new();
        let (reached, release) = h.pause_after_append_call(1);
        let writer = h.clone();
        let task = tokio::spawn(async move {
            writer
                .append_object(
                    "store-v1/test/copies/one.json",
                    BlobBody::from_bytes(b"first".to_vec()),
                    &no_progress(),
                )
                .await
        });

        reached.notified().await;
        assert_eq!(h.append_count(), 1);
        assert_eq!(
            h.get_appended("store-v1/test/copies/one.json"),
            Some(b"first".to_vec()),
            "the physical copy is inspectable while the append call is paused",
        );
        release.notify_one();
        let locator = task.await.unwrap().unwrap();

        h.replace_appended_candidate(&locator, b"replacement".to_vec());
        assert_eq!(
            h.get_appended(locator.logical_key()),
            Some(b"replacement".to_vec())
        );
        h.remove_appended_candidate(&locator);
        assert_eq!(h.get_appended(locator.logical_key()), None);
    }
}
