//! Counting the operations a run asks of its provider.
//!
//! A stage's wall time says how long it waited, not what it waited on, and the
//! two shapes want opposite fixes: one slow transfer is a size problem, two
//! hundred fast ones are a round-trip problem no faster network will help. So
//! every provider call is counted, and the stage timings report the count
//! beside the time.
//!
//! The count is taken at the [`CloudHome`]/[`ExactSlotStorage`] boundary, which
//! is the one place every provider call crosses, so nothing is counted twice
//! and nothing is missed. What it counts is *operations a caller asked for*,
//! not HTTP round trips: a listing that pages, a write that goes multipart, and
//! a delete that reads back to prove absence are each one operation here and
//! several requests underneath. That is the granularity the budget is written
//! in — whether a stage does a fixed number of operations or one per commit —
//! and a stage whose count is small while its time is large is a paging or
//! streaming call, which is worth telling apart rather than hiding in a total.
//!
//! Every method is forwarded, including the ones the traits give defaults for.
//! A decorator that inherited a default instead of forwarding it would silently
//! replace a provider's override: Google Drive mints its own object ids in
//! `allocate_slot`, and inheriting the logical-key default would break it. The
//! one method that is answered rather than forwarded is
//! [`CloudHome::provider_requests`], which is how a run finds the counter.
//!
//! The wrapping happens where the home is built, not where a
//! [`CloudSyncConnection`](crate::CloudSyncConnection) is, because one home
//! outlives and is shared by several connections — a device join opens a
//! plaintext one to pin the Store root and walk the membership chain, then an
//! encrypted one for everything after, over the same provider. Counting per
//! connection would split that join's operations across two totals and start
//! the one it reports from zero.

use super::*;
use coven_foundation::stage_timing::ProviderRequests;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The running total of provider operations, shared between the home that
/// counts them and whoever reports them. Cloning shares the total rather than
/// copying it, which is what lets the home keep counting into the same number a
/// run is already reading.
#[derive(Clone, Debug, Default)]
struct ProviderRequestCount(Arc<AtomicU64>);

impl ProviderRequestCount {
    fn record(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl ProviderRequests for ProviderRequestCount {
    fn issued(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A cloud home that counts what is asked of it and forwards everything else.
pub struct CountingCloudHome {
    inner: Arc<dyn ExactCloudHome>,
    count: ProviderRequestCount,
}

impl CountingCloudHome {
    /// Wraps `inner` so every operation asked of it is counted. Whoever holds
    /// the wrapped home reaches the running total through
    /// [`CloudHome::provider_requests`], so the counter needs no separate route
    /// from here to the runs that report it.
    ///
    /// Returns the wrapper itself rather than a boxed or reference-counted
    /// home, because the three places that build a production home hand it on
    /// in different containers.
    pub fn new(inner: Arc<dyn ExactCloudHome>) -> Self {
        Self {
            inner,
            count: ProviderRequestCount::default(),
        }
    }

    fn counted(&self) -> &dyn ExactCloudHome {
        self.count.record();
        self.inner.as_ref()
    }
}

#[async_trait]
impl ExactSlotStorage for CountingCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        self.counted().provider_binding().await
    }

    async fn cross_principal_evidence(
        &self,
    ) -> Result<coven_protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        self.counted().cross_principal_evidence().await
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        self.counted().allocate_slot(logical_key).await
    }

    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        self.counted().list_slots(prefix).await
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        control: &UploadControl,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
        self.counted().create_at(upload, control).await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        self.counted().read_at(slot).await
    }

    async fn observe_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<coven_protocol::objects::ExactObjectRef>, CloudHomeError> {
        self.counted().observe_at(slot).await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        self.counted().read_range_at(slot, start, end).await
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
        progress: DownloadProgress,
    ) -> Result<(), CloudFileReadError> {
        self.counted()
            .read_at_to_file(slot, destination, progress)
            .await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.counted().delete_at(slot).await
    }

    async fn delete_and_verify_absent(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.counted().delete_and_verify_absent(slot).await
    }
}

#[async_trait]
impl CloudHome for CountingCloudHome {
    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.counted().probe().await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.counted().put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.counted().open_multipart(key, total_len).await
    }

    /// A getter, not a request.
    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    /// Answered here rather than forwarded: this is the counter, so this is
    /// what a run reporting counts is looking for.
    fn provider_requests(&self) -> Option<Arc<dyn ProviderRequests>> {
        Some(Arc::new(self.count.clone()))
    }

    async fn write(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress,
    ) -> Result<(), CloudHomeError> {
        self.counted().write(key, body, progress).await
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.counted().read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.counted().read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.counted().list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.counted().delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.counted().exists(key).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        self.counted().set_access(desired).await
    }
}
