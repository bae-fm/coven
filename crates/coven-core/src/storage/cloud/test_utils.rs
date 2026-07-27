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

use super::{
    BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudFileReadError, CloudHome,
    CloudHomeError, ExactSlotStorage, ObjectSlot, PartSink, UploadProgress,
};

#[derive(Clone)]
struct AppendPause {
    call: usize,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

struct ExactStreamReadGuard {
    inflight: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ProbePause {
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl Drop for ExactStreamReadGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }
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
    provider_binding: crate::sync::storage::ResolvedProviderBinding,
    writes: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    exact_slot_allocations: Arc<AtomicUsize>,
    deletes: Arc<Mutex<Vec<String>>>,
    fail_writes: Arc<AtomicBool>,
    fail_next_range_reads: Arc<AtomicUsize>,
    sort_listings: Arc<AtomicBool>,
    exact_create_count: Arc<AtomicUsize>,
    fail_exact_create_before: Arc<AtomicUsize>,
    fail_exact_create_after: Arc<AtomicUsize>,
    corrupt_exact_readback: Arc<AtomicUsize>,
    exact_create_pause: Arc<Mutex<Option<AppendPause>>>,
    probe_pause: Arc<Mutex<Option<ProbePause>>>,
    exact_full_read_count: Arc<AtomicUsize>,
    exact_stream_read_count: Arc<AtomicUsize>,
    exact_reads: Arc<Mutex<Vec<ObjectSlot>>>,
    exact_stream_read_inflight: Arc<AtomicUsize>,
    exact_stream_read_max_inflight: Arc<AtomicUsize>,
    exact_stream_read_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
    exact_delete_count: Arc<AtomicUsize>,
    /// Every ranged exact read this home has served, as `(start, end)` stored
    /// offsets. What a test counts to say a read cost the bytes it asked for and
    /// no more — a full read is counted separately, by `exact_full_read_count`
    /// and `exact_stream_read_count`, so reintroducing a whole-object fetch
    /// shows up as a full read rather than hiding inside the range total.
    exact_range_reads: Arc<Mutex<Vec<(u64, u64)>>>,
    fail_exact_delete_on: Arc<AtomicUsize>,
    fail_exact_delete_of: Arc<Mutex<Option<TargetedDeleteFailure>>>,
}

/// Fail the `countdown`-th delete of an object whose key is in `keys`, counting
/// only those deletes. Set by [`InMemoryCloudHome::fail_nth_exact_delete_of`].
struct TargetedDeleteFailure {
    keys: std::collections::HashSet<String>,
    countdown: usize,
}

impl InMemoryCloudHome {
    pub fn new() -> Self {
        Self {
            provider_binding: crate::sync::storage::ResolvedProviderBinding {
                store: crate::sync::storage::StoreProviderBinding::S3 {
                    endpoint: crate::sync::storage::S3EndpointBinding::Custom {
                        origin: "https://in-memory.invalid".to_string(),
                    },
                    region: "test".to_string(),
                    bucket: "in-memory".to_string(),
                    key_prefix: None,
                },
                device: crate::sync::storage::ProviderDeviceBinding {
                    principal: crate::sync::storage::ProviderPrincipalId::CustomS3Credential {
                        access_key_id_hash: crate::sync::store_commit::ObjectHash::digest(
                            b"coven.s3-access-key-id.v1\0in-memory",
                        ),
                    },
                },
            },
            writes: Arc::new(Mutex::new(HashMap::new())),
            exact_slot_allocations: Arc::new(AtomicUsize::new(0)),
            deletes: Arc::new(Mutex::new(Vec::new())),
            fail_writes: Arc::new(AtomicBool::new(false)),
            fail_next_range_reads: Arc::new(AtomicUsize::new(0)),
            sort_listings: Arc::new(AtomicBool::new(false)),
            exact_create_count: Arc::new(AtomicUsize::new(0)),
            fail_exact_create_before: Arc::new(AtomicUsize::new(0)),
            fail_exact_create_after: Arc::new(AtomicUsize::new(0)),
            corrupt_exact_readback: Arc::new(AtomicUsize::new(0)),
            exact_create_pause: Arc::new(Mutex::new(None)),
            probe_pause: Arc::new(Mutex::new(None)),
            exact_full_read_count: Arc::new(AtomicUsize::new(0)),
            exact_stream_read_count: Arc::new(AtomicUsize::new(0)),
            exact_reads: Arc::new(Mutex::new(Vec::new())),
            exact_stream_read_inflight: Arc::new(AtomicUsize::new(0)),
            exact_stream_read_max_inflight: Arc::new(AtomicUsize::new(0)),
            exact_stream_read_barrier: Arc::new(Mutex::new(None)),
            exact_delete_count: Arc::new(AtomicUsize::new(0)),
            exact_range_reads: Arc::new(Mutex::new(Vec::new())),
            fail_exact_delete_on: Arc::new(AtomicUsize::new(0)),
            fail_exact_delete_of: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_provider_binding(
        mut self,
        binding: crate::sync::storage::ResolvedProviderBinding,
    ) -> Self {
        binding
            .validate()
            .expect("in-memory provider binding must be valid");
        self.provider_binding = binding;
        self
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

    /// Reset the exact-create counter and fail before the selected call stores bytes.
    pub fn fail_exact_create_before_call(&self, call: usize) {
        assert!(call > 0, "create call numbers are 1-based");
        self.exact_create_count.store(0, Ordering::SeqCst);
        self.fail_exact_create_before.store(call, Ordering::SeqCst);
    }

    /// Reset the exact-create counter and lose the response after the selected create.
    pub fn fail_exact_create_after_call(&self, call: usize) {
        assert!(call > 0, "create call numbers are 1-based");
        self.exact_create_count.store(0, Ordering::SeqCst);
        self.fail_exact_create_after.store(call, Ordering::SeqCst);
    }

    /// Replace the selected exact object's bytes before its verification read.
    pub fn corrupt_exact_readback_on_call(&self, call: usize) {
        assert!(call > 0, "create call numbers are 1-based");
        self.exact_create_count.store(0, Ordering::SeqCst);
        self.corrupt_exact_readback.store(call, Ordering::SeqCst);
    }

    /// Pause after the selected exact create is physically visible.
    pub fn pause_after_exact_create_call(
        &self,
        call: usize,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        assert!(call > 0, "create call numbers are 1-based");
        self.exact_create_count.store(0, Ordering::SeqCst);
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.exact_create_pause.lock().unwrap() = Some(AppendPause {
            call,
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    /// Pause the next reachability probe after it starts and before it succeeds.
    pub fn pause_next_probe(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.probe_pause.lock().unwrap() = Some(ProbePause {
            reached: reached.clone(),
            release: release.clone(),
        });
        (reached, release)
    }

    pub fn exact_create_count(&self) -> usize {
        self.exact_create_count.load(Ordering::SeqCst)
    }

    pub fn exact_full_read_count(&self) -> usize {
        self.exact_full_read_count.load(Ordering::SeqCst)
    }

    /// Every ranged exact read served so far, as `(start, end)` stored offsets.
    pub fn exact_range_reads(&self) -> Vec<(u64, u64)> {
        self.exact_range_reads.lock().unwrap().clone()
    }

    /// Total stored bytes ranged reads have transferred.
    pub fn exact_range_read_bytes(&self) -> u64 {
        self.exact_range_reads
            .lock()
            .unwrap()
            .iter()
            .map(|(start, end)| end - start)
            .sum()
    }

    pub fn clear_exact_range_reads(&self) {
        self.exact_range_reads.lock().unwrap().clear();
    }

    pub fn exact_stream_read_count(&self) -> usize {
        self.exact_stream_read_count.load(Ordering::SeqCst)
    }

    pub fn exact_reads(&self) -> Vec<ObjectSlot> {
        self.exact_reads.lock().unwrap().clone()
    }

    pub fn clear_exact_reads(&self) {
        self.exact_reads.lock().unwrap().clear();
    }

    pub fn arm_exact_stream_read_concurrency_probe(&self, width: usize) {
        assert!(width > 0, "exact stream read probe width must be positive");
        self.exact_stream_read_inflight.store(0, Ordering::SeqCst);
        self.exact_stream_read_max_inflight
            .store(0, Ordering::SeqCst);
        *self.exact_stream_read_barrier.lock().unwrap() =
            Some(Arc::new(tokio::sync::Barrier::new(width)));
    }

    pub fn exact_stream_read_max_inflight(&self) -> usize {
        self.exact_stream_read_max_inflight.load(Ordering::SeqCst)
    }

    pub fn exact_delete_count(&self) -> usize {
        self.exact_delete_count.load(Ordering::SeqCst)
    }

    pub fn fail_exact_delete_on_call(&self, call: usize) {
        assert!(call > 0, "exact-delete call numbers are 1-based");
        self.exact_delete_count.store(0, Ordering::SeqCst);
        self.fail_exact_delete_on.store(call, Ordering::SeqCst);
    }

    /// Fail the `nth` (1-based) delete of an object among `slots`, counting only
    /// deletes of those objects, then disarm. Unlike `fail_exact_delete_on_call`,
    /// which counts every exact delete (probes, candidate cleanup), this counts
    /// only the identities that matter, so "fail the 2nd package delete" lands
    /// deterministically however many unrelated deletes interleave and whatever
    /// order the two package deletes arrive in.
    pub fn fail_nth_exact_delete_of(&self, slots: &[&ObjectSlot], nth: usize) {
        assert!(nth > 0, "targeted delete ordinals are 1-based");
        let keys = slots
            .iter()
            .map(|slot| Self::exact_storage_key(slot).expect("test exact slot is valid"))
            .collect();
        *self.fail_exact_delete_of.lock().unwrap() = Some(TargetedDeleteFailure {
            keys,
            countdown: nth,
        });
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

    /// Insert caller-selected bytes at one exact logical slot.
    pub fn insert_exact_object(&self, logical_key: &str, bytes: Vec<u8>) -> ObjectSlot {
        let slot =
            ObjectSlot::logical(logical_key.to_string()).expect("test logical key is non-empty");
        self.writes
            .lock()
            .unwrap()
            .insert(logical_key.to_string(), bytes);
        slot
    }

    /// Snapshot the bytes stored at one exact slot, or `None` if absent.
    pub fn stored_exact_bytes(&self, slot: &ObjectSlot) -> Option<Vec<u8>> {
        let key = Self::exact_storage_key(slot).expect("test exact slot is valid");
        self.writes.lock().unwrap().get(&key).cloned()
    }

    /// Re-insert bytes at one exact slot, restoring an object dropped by
    /// [`remove_exact_object`](Self::remove_exact_object).
    pub fn restore_exact_object(&self, slot: &ObjectSlot, bytes: Vec<u8>) {
        let key = Self::exact_storage_key(slot).expect("test exact slot is valid");
        self.writes.lock().unwrap().insert(key, bytes);
    }

    /// Remove one exact object without recording a protocol delete.
    pub fn remove_exact_object(&self, slot: &ObjectSlot) {
        let key = Self::exact_storage_key(slot).expect("test exact slot is valid");
        self.writes.lock().unwrap().remove(&key);
    }

    /// The bytes currently stored at one exact slot, without counting a read.
    pub fn stored_exact_object(&self, slot: &ObjectSlot) -> Vec<u8> {
        self.writes
            .lock()
            .unwrap()
            .get(&Self::exact_storage_key(slot).expect("test exact slot is valid"))
            .cloned()
            .expect("exact slot exists")
    }

    /// Replace bytes at one exact slot without changing its locator.
    pub fn replace_exact_object(&self, slot: &ObjectSlot, bytes: Vec<u8>) {
        let previous = self.writes.lock().unwrap().insert(
            Self::exact_storage_key(slot).expect("test exact slot is valid"),
            bytes,
        );
        assert!(previous.is_some(), "exact slot exists");
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

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        self.writes.lock().unwrap().insert(self.key, self.buf);
        Ok(())
    }
}

impl InMemoryCloudHome {
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
    fn validate_exact_slot(slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.validate()?;
        Ok(())
    }

    fn exact_storage_key(slot: &ObjectSlot) -> Result<String, CloudHomeError> {
        Self::validate_exact_slot(slot)?;
        Ok(match slot.physical() {
            super::PhysicalObjectLocator::LogicalKey => slot.logical_key().to_string(),
            super::PhysicalObjectLocator::Opaque(provider_id) => {
                format!("{}#exact#{provider_id}", slot.logical_key())
            }
        })
    }

    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "InMemoryCloudHome: armed write failure".into(),
            ));
        }
        let key = Self::exact_storage_key(slot)?;
        let call = self.exact_create_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_exact_create_before.load(Ordering::SeqCst) == call {
            self.fail_exact_create_before.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced failure before exact create call {call}"
            )));
        }
        let bytes = body.collect().await?;
        progress(bytes.len() as u64);
        {
            let mut writes = self.writes.lock().unwrap();
            if writes.contains_key(&key) {
                return Err(CloudHomeError::AlreadyExists(key));
            }
            writes.insert(key.clone(), bytes);
        }
        if self.corrupt_exact_readback.load(Ordering::SeqCst) == call {
            self.corrupt_exact_readback.store(0, Ordering::SeqCst);
            self.writes
                .lock()
                .unwrap()
                .insert(key.clone(), b"corrupt readback".to_vec());
        }
        let pause = self
            .exact_create_pause
            .lock()
            .unwrap()
            .clone()
            .filter(|pause| pause.call == call);
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.release.notified().await;
            self.exact_create_pause.lock().unwrap().take();
        }
        if self.fail_exact_create_after.load(Ordering::SeqCst) == call {
            self.fail_exact_create_after.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced failure after exact create call {call}"
            )));
        }
        Ok(())
    }

    async fn read_exact(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        self.exact_full_read_count.fetch_add(1, Ordering::SeqCst);
        let key = Self::exact_storage_key(slot)?;
        self.exact_reads.lock().unwrap().push(slot.clone());
        self.writes
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(slot.logical_key().to_string()))
    }

    async fn read_exact_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), CloudFileReadError> {
        self.exact_stream_read_count.fetch_add(1, Ordering::SeqCst);
        self.exact_reads.lock().unwrap().push(slot.clone());
        let inflight = self
            .exact_stream_read_inflight
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        self.exact_stream_read_max_inflight
            .fetch_max(inflight, Ordering::SeqCst);
        let _guard = ExactStreamReadGuard {
            inflight: self.exact_stream_read_inflight.clone(),
        };
        let barrier = self.exact_stream_read_barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        let key = Self::exact_storage_key(slot)?;
        let bytes = self
            .writes
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(slot.logical_key().to_string()))?;
        let stream = futures_util::stream::once(async move { Ok(bytes::Bytes::from(bytes)) });
        super::write_cloud_object_stream(destination, Box::pin(stream)).await?;
        Ok(())
    }

    async fn delete_exact(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        let key = Self::exact_storage_key(slot)?;
        let call = self.exact_delete_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_exact_delete_on.load(Ordering::SeqCst) == call {
            self.fail_exact_delete_on.store(0, Ordering::SeqCst);
            return Err(CloudHomeError::Transport(format!(
                "InMemoryCloudHome: forced exact delete failure on call {call}"
            )));
        }
        {
            let mut targeted = self.fail_exact_delete_of.lock().unwrap();
            if let Some(failure) = targeted.as_mut() {
                if failure.keys.contains(&key) {
                    failure.countdown -= 1;
                    if failure.countdown == 0 {
                        *targeted = None;
                        return Err(CloudHomeError::Transport(format!(
                            "InMemoryCloudHome: forced exact delete failure of {key}"
                        )));
                    }
                }
            }
        }
        self.writes.lock().unwrap().remove(&key);
        self.deletes.lock().unwrap().push(key);
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
        Ok(match desired {
            super::CloudAccessState::Present { .. } => {
                super::CloudAccessOutcome::Present(super::CloudHomeJoinInfo::S3 {
                    bucket: "in-memory".to_string(),
                    region: "test".to_string(),
                    endpoint: Some("https://in-memory.invalid".to_string()),
                    access_key: "in-memory".to_string(),
                    secret_key: "in-memory".to_string(),
                    key_prefix: None,
                })
            }
            super::CloudAccessState::Absent { .. } => {
                super::CloudAccessOutcome::Absent(super::RevokeOutcome::Unsupported)
            }
        })
    }
}

#[async_trait]
impl CloudHome for InMemoryCloudHome {
    fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
        Some(self)
    }

    async fn probe(&self) -> Result<(), CloudHomeError> {
        let pause = self.probe_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.release.notified().await;
        }
        Ok(())
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        InMemoryCloudHome::put_object(self, key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        InMemoryCloudHome::open_multipart(self, key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        InMemoryCloudHome::multipart_threshold(self)
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        InMemoryCloudHome::read(self, key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        InMemoryCloudHome::read_range(self, key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        InMemoryCloudHome::list(self, prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        InMemoryCloudHome::delete(self, key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        InMemoryCloudHome::exists(self, key).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        InMemoryCloudHome::set_access(self, desired).await
    }
}

#[async_trait]
impl ExactSlotStorage for InMemoryCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, CloudHomeError> {
        Ok(self.provider_binding.clone())
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        match &self.provider_binding.store {
            crate::sync::storage::StoreProviderBinding::GoogleDrive { .. } => {
                let allocation = self.exact_slot_allocations.fetch_add(1, Ordering::SeqCst) + 1;
                ObjectSlot::opaque(logical_key.to_string(), format!("in-memory-{allocation}"))
            }
            crate::sync::storage::StoreProviderBinding::S3 { .. }
            | crate::sync::storage::StoreProviderBinding::Dropbox { .. }
            | crate::sync::storage::StoreProviderBinding::OneDrive { .. }
            | crate::sync::storage::StoreProviderBinding::CloudKit { .. } => {
                ObjectSlot::logical(logical_key.to_string())
            }
        }
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        InMemoryCloudHome::create_at_slot(self, slot, body, progress).await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        InMemoryCloudHome::read_exact(self, slot).await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        // Served straight out of the bucket rather than through `read_exact`, so
        // the full-read counter keeps meaning "something fetched a whole object"
        // and a ranged read never inflates it.
        let key = Self::exact_storage_key(slot)?;
        let bytes = self
            .writes
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(slot.logical_key().to_string()))?;
        // A range past the object's end is refused, not clamped: a short answer
        // to a range request is the provider ignoring it, which a caller must
        // see rather than splice.
        let window = bytes
            .get(start as usize..end as usize)
            .ok_or_else(|| {
                CloudHomeError::NotFound(format!(
                    "range {start}..{end} past the {} bytes of {}",
                    bytes.len(),
                    slot.logical_key()
                ))
            })?
            .to_vec();
        self.exact_range_reads.lock().unwrap().push((start, end));
        Ok(window)
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), CloudFileReadError> {
        InMemoryCloudHome::read_exact_to_file(self, slot, destination).await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        InMemoryCloudHome::delete_exact(self, slot).await
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
    async fn exact_create_is_visible_before_a_lost_response() {
        let h = InMemoryCloudHome::new();
        let slot = h.allocate_slot("store-v1/test/one.json").await.unwrap();
        let (reached, release) = h.pause_after_exact_create_call(1);
        let writer = h.clone();
        let writer_slot = slot.clone();
        let task = tokio::spawn(async move {
            writer
                .create_at(
                    &writer_slot,
                    BlobBody::from_bytes(b"first".to_vec()),
                    &no_progress(),
                )
                .await
        });

        reached.notified().await;
        assert_eq!(h.exact_create_count(), 1);
        assert_eq!(h.read_at(&slot).await.unwrap(), b"first");
        release.notify_one();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn exact_create_never_overwrites() {
        let h = InMemoryCloudHome::new();
        let slot = h.allocate_slot("store-v1/test/one.json").await.unwrap();
        h.create_at(
            &slot,
            BlobBody::from_bytes(b"winner".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

        assert!(matches!(
            h.create_at(
                &slot,
                BlobBody::from_bytes(b"loser".to_vec()),
                &no_progress(),
            )
            .await,
            Err(CloudHomeError::AlreadyExists(_))
        ));
        assert_eq!(h.read_at(&slot).await.unwrap(), b"winner");
    }

    #[tokio::test]
    async fn google_drive_exact_slots_with_one_logical_key_remain_independent() {
        let h = InMemoryCloudHome::new().with_provider_binding(
            crate::sync::storage::ResolvedProviderBinding {
                store: crate::sync::storage::StoreProviderBinding::GoogleDrive {
                    corpus: crate::sync::storage::GoogleDriveCorpus::SharedDrive {
                        drive_id: "drive-id".to_string(),
                        folder_id: "folder-id".to_string(),
                    },
                },
                device: crate::sync::storage::ProviderDeviceBinding {
                    principal: crate::sync::storage::ProviderPrincipalId::GoogleDrive {
                        permission_id: "permission-id".to_string(),
                    },
                },
            },
        );
        let first = h.allocate_slot("store-v1/test/one.json").await.unwrap();
        let second = h.allocate_slot("store-v1/test/one.json").await.unwrap();
        assert_ne!(first, second);
        h.create_at(
            &first,
            BlobBody::from_bytes(b"first".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        h.create_at(
            &second,
            BlobBody::from_bytes(b"second".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
        assert_eq!(h.read_at(&first).await.unwrap(), b"first");
        assert_eq!(h.read_at(&second).await.unwrap(), b"second");
        h.delete_at(&first).await.unwrap();
        assert!(matches!(
            h.read_at(&first).await,
            Err(CloudHomeError::NotFound(_))
        ));
        assert_eq!(h.read_at(&second).await.unwrap(), b"second");
    }

    #[tokio::test]
    async fn google_drive_exact_slots_with_different_logical_keys_have_distinct_file_ids() {
        let h = InMemoryCloudHome::new().with_provider_binding(
            crate::sync::storage::ResolvedProviderBinding {
                store: crate::sync::storage::StoreProviderBinding::GoogleDrive {
                    corpus: crate::sync::storage::GoogleDriveCorpus::SharedDrive {
                        drive_id: "drive-id".to_string(),
                        folder_id: "folder-id".to_string(),
                    },
                },
                device: crate::sync::storage::ProviderDeviceBinding {
                    principal: crate::sync::storage::ProviderPrincipalId::GoogleDrive {
                        permission_id: "permission-id".to_string(),
                    },
                },
            },
        );

        let first = h.allocate_slot("store-v1/test/one.json").await.unwrap();
        let second = h.allocate_slot("store-v1/test/two.json").await.unwrap();

        assert_ne!(first.physical(), second.physical());
    }

    #[tokio::test]
    async fn access_matches_the_in_memory_s3_binding() {
        let h = InMemoryCloudHome::new();
        let desired = CloudAccessState::Present {
            member_pubkey: "member".to_string(),
            provider_account_email: None,
        };

        let first = h.set_access(desired.clone()).await.unwrap();
        let second = h.set_access(desired).await.unwrap();
        let expected = CloudAccessOutcome::Present(super::super::CloudHomeJoinInfo::S3 {
            bucket: "in-memory".to_string(),
            region: "test".to_string(),
            endpoint: Some("https://in-memory.invalid".to_string()),
            access_key: "in-memory".to_string(),
            secret_key: "in-memory".to_string(),
            key_prefix: None,
        });
        assert_eq!(first, expected);
        assert_eq!(second, expected);
        assert_eq!(
            h.set_access(CloudAccessState::Absent {
                member_pubkey: "member".to_string(),
                provider_account_email: None,
            })
            .await
            .unwrap(),
            CloudAccessOutcome::Absent(super::super::RevokeOutcome::Unsupported)
        );
    }
}
