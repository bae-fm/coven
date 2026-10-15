use super::*;
use crate::store_log_sync::tests::{store, Device};
use coven_foundation::{
    clock::{Clock, FixedClock},
    id_source::DeviceId,
};
use coven_storage::{
    test_utils::MemoryStorage, AccessGrant, ByteRange, ObjectPath, ObjectPrefix, RetainedAccess,
    Storage, StorageConfig, StorageError, StoredObject, UploadSession,
};
use serde_json::json;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

const DAY: u64 = 24 * 60 * 60;

fn time(seconds: u64) -> StorageTime {
    StorageTime::new(seconds, 0).unwrap()
}

#[test]
fn a_miss_is_recent_strictly_before_twenty_nine_days() {
    let elapsed = Duration::from_secs;
    assert!(needs_snapshots(None, time(0), elapsed(0)));
    assert!(!needs_snapshots(
        Some(time(0)),
        time(29 * DAY - 1),
        elapsed(0)
    ));
    assert!(needs_snapshots(Some(time(0)), time(29 * DAY), elapsed(0)));
    assert!(needs_snapshots(Some(time(0)), time(28 * DAY), elapsed(DAY)));
    assert!(!needs_snapshots(
        Some(time(0)),
        StorageTime::new(29 * DAY - 1, 999_999_999).unwrap(),
        elapsed(0)
    ));
    // Storage time is refreshed a day before the window closes.
    assert!(!refresh_due(Some(time(0)), time(28 * DAY - 1), elapsed(0)));
    assert!(refresh_due(Some(time(0)), time(28 * DAY), elapsed(0)));
}

#[test]
fn only_a_clock_sample_with_its_monotonic_start_certifies_a_miss() {
    let observation = Arc::new(Mutex::new(Observation::default()));
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    assert!(!miss_is_recent(&observation, clock.as_ref(), Some(time(0))));
    // An object's stored time is a lower bound, not a fresh sample.
    observation.lock().unwrap().observe(&StoredObject {
        path: ObjectPath::clock(store(1), DeviceId(1)),
        size: 1,
        stored_at: time(DAY),
        time_resolution: Duration::from_secs(1),
        object_id: "id".into(),
        revision: "1".into(),
        checksum: None,
    });
    assert!(!miss_is_recent(&observation, clock.as_ref(), Some(time(0))));
    observation
        .lock()
        .unwrap()
        .clock_observed(time(DAY), clock.elapsed());
    assert!(miss_is_recent(&observation, clock.as_ref(), Some(time(0))));
    // Suspension counts: 28 days asleep reach the deadline at the next use.
    clock.set(UNIX_EPOCH + Duration::from_secs(28 * DAY));
    assert!(!miss_is_recent(&observation, clock.as_ref(), Some(time(0))));
    // A later checkpoint, or this session's snapshot discovery, reopens it.
    assert!(miss_is_recent(
        &observation,
        clock.as_ref(),
        Some(time(2 * DAY))
    ));
    observation
        .lock()
        .unwrap()
        .snapshots_discovered(time(2 * DAY));
    assert!(miss_is_recent(&observation, clock.as_ref(), Some(time(0))));
    assert!(miss_is_recent(&observation, clock.as_ref(), None));
}

/// IO `needsSnapshots` on integer-second samples, including the exact
/// boundary, `pass_crosses_deadline` and the clock-jump counterexample.
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_needs_snapshots() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for saved in [None, Some(0), Some(DAY)] {
        for observed in [0, 1, 28 * DAY, 29 * DAY - 1, 29 * DAY, 30 * DAY] {
            for elapsed in [0, 1, DAY, 3 * DAY] {
                cases.push(
                    json!({"check": "needs", "saved": saved, "observed": observed,
                    "elapsed": elapsed}),
                );
                actual.push(json!(needs_snapshots(
                    saved.map(time),
                    time(observed),
                    Duration::from_secs(elapsed)
                )));
            }
        }
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// Advances the reader's monotonic clock by `step` on each probe of one
/// writer's log, like a request that takes that long.
struct SlowWrites {
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    writer: DeviceId,
    step: Duration,
}

#[async_trait::async_trait]
impl Storage for SlowWrites {
    fn config(&self) -> StorageConfig {
        self.storage.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.storage.single_request_limit()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.storage.create(path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.storage.replace(path, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        if path
            .write_position()
            .is_some_and(|(writer, _)| writer == self.writer)
        {
            self.clock.set(self.clock.now() + self.step);
        }
        self.storage.status(path).await
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<coven_storage::ObjectStream<'_>, StorageError> {
        self.storage.stream_object(path, range).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.storage.list(prefix).await
    }
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<DeviceId>, StorageError> {
        self.storage.list_writers(store).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.storage.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.storage.grant_access(account).await
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.storage.revoke_access(account).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.storage.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.resume_upload(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.storage.upload_part(session, bytes).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.finish_upload(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.storage.abort_upload(session).await
    }
}

async fn note(device: &Device, n: u64) {
    device
        .db
        .write(move |context| {
            context.execute(
                "INSERT INTO notes VALUES(?1,'title','body')",
                [n.to_string()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

/// IO `readRecentLog`, `accepted_miss_is_recent` and `recent_return_complete`:
/// the real write discovery judges each terminal miss when it is used, with
/// elapsed time growing while it reads.
#[tokio::test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
async fn io_differential_recent_return() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for count in 0..=2u64 {
        for deleted in [vec![], vec![1u64]] {
            if deleted.iter().any(|n| *n > count) {
                continue;
            }
            for saved in [None, Some(0)] {
                for observed in [1, 28 * DAY, 29 * DAY - 1, 29 * DAY] {
                    for (base, step) in [(0, 0), (0, DAY), (2 * DAY, DAY)] {
                        let shared = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
                        let storage = Arc::new(
                            MemoryStorage::builder()
                                .clock(shared.clone())
                                .build()
                                .unwrap(),
                        );
                        let mut devices =
                            crate::device_log_sync::tests::group(storage.clone(), 2).await;
                        let mut reader = devices.pop().unwrap();
                        let mut author = devices.pop().unwrap();
                        for n in 0..count {
                            note(&author, n).await;
                        }
                        author.writes.upload_writes().await.unwrap();
                        for n in &deleted {
                            storage
                                .delete(&crate::write_seal::path(
                                    crate::device_log_sync::tests::STORE_ID,
                                    coven_merge::WriteId {
                                        device: DeviceId(1),
                                        number: *n,
                                    },
                                ))
                                .await
                                .unwrap();
                        }
                        if let Some(saved) = saved {
                            reader.db.save_return_checkpoint(time(saved)).await.unwrap();
                        }
                        shared.set(UNIX_EPOCH + Duration::from_secs(observed));
                        reader
                            .sync
                            .observe_clock(&reader.db.local_store_log().await.unwrap())
                            .await
                            .unwrap();
                        reader
                            .clock
                            .set(reader.clock.now() + Duration::from_secs(base));
                        let _pass = reader.sync.begin_pass(&mut reader.writes);
                        reader.writes.set_storage(Some(Arc::new(SlowWrites {
                            storage: storage.clone(),
                            clock: reader.clock.clone(),
                            writer: DeviceId(1),
                            step: Duration::from_secs(step),
                        })));
                        let discovery = reader.writes.download_writes().await.unwrap();
                        let position = reader
                            .db
                            .sync_state(Vec::new())
                            .await
                            .unwrap()
                            .positions
                            .0
                            .iter()
                            .find(|p| p.device == DeviceId(1))
                            .map_or(0, |p| p.number);
                        cases.push(
                            json!({"check": "recent", "count": count, "deleted": deleted,
                            "saved": saved, "observed": observed, "base": base, "step": step}),
                        );
                        actual.push(json!({
                            "stop": if discovery.needs_snapshots { "snapshots" } else { "miss" },
                            "position": position,
                        }));
                    }
                }
            }
        }
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// A write needing a cause past its writer's miss is required history: the
/// pass consults a fresh snapshot catalog before treating it as pending.
#[tokio::test]
async fn a_cause_past_its_writers_miss_requires_snapshot_discovery() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut devices = crate::device_log_sync::tests::group(storage.clone(), 3).await;
    note(&devices[1], 1).await;
    let cause = devices[1].db.test_queued_writes().await.unwrap().remove(0);
    devices[2]
        .db
        .apply_downloaded::<crate::PendingRecord>(
            cause.into(),
            coven_database::test_utils::published(coven_format::value::StorageTime::UNIX_EPOCH),
        )
        .await
        .unwrap();
    note(&devices[2], 2).await;
    devices[2].writes.upload_writes().await.unwrap();
    let reader = &mut devices[0];
    reader
        .sync
        .observe_clock(&reader.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    reader
        .db
        .save_return_checkpoint(StorageTime::from_system_time(reader.clock.now()).unwrap())
        .await
        .unwrap();
    let _pass = reader.sync.begin_pass(&mut reader.writes);
    let discovery = reader.writes.download_writes().await.unwrap();
    assert!(discovery.needs_snapshots);
    assert!(!discovery.complete);
}
