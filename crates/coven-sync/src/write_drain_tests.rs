//! Retired devices are drained once after their retirement's finality wait (§6).

use crate::device_log_sync::tests::{group, rows, schema, sql, STORE_ID};
use crate::store_log_sync::tests::Device;
use crate::SyncError;
use coven_database::{RowIdentity, SyncedTable};
use coven_format::store_log::StoreChange;
use coven_foundation::clock::FixedClock;
use coven_foundation::id_source::DeviceId;
use coven_storage::test_utils::MemoryRequest;
use coven_storage::test_utils::MemoryStorage;
use coven_storage::StorageFailure;
use coven_storage::{ObjectPath, Storage};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

const DAY: u64 = 24 * 60 * 60;
/// Storage seconds after which the removal published at second 10 is final:
/// `T′ = T − one hour − δ` must leave a horizon past it (§9).
const FINAL: u64 = 30 * DAY + 3612;
const WAIT: u64 = DAY + 300;

fn second(n: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(n)
}

struct Removed {
    clock: Arc<FixedClock>,
    storage: Arc<MemoryStorage>,
    ana: Device,
}

/// Ana's group with Ben's laptop (device 2), which uploaded one write before
/// Ana removed it at storage second 10. Ana has applied that write.
async fn removed() -> Removed {
    let clock = Arc::new(FixedClock::new(second(1)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(1024 * 1024, 64 * 1024)
            .clock(clock.clone())
            .time_resolution(Duration::from_secs(1))
            .build()
            .unwrap(),
    );
    let mut devices = group(storage.clone(), 2).await;
    let mut ben = devices.pop().unwrap();
    let mut ana = devices.pop().unwrap();
    sql(&ben.db, "INSERT INTO notes VALUES('one','title','body')").await;
    ben.writes.upload_writes().await.unwrap();
    clock.set(second(10));
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: DeviceId(2),
        })
        .await
        .unwrap();
    Removed {
        clock,
        storage,
        ana,
    }
}

/// Requests naming one of `writer`'s three logs.
fn probes(trace: &[MemoryRequest], writer: DeviceId) -> usize {
    trace
        .iter()
        .filter(|request| match request {
            MemoryRequest::Read(path, _) | MemoryRequest::Status(path) => {
                path.store_log_position().is_some_and(|(d, _)| d == writer)
                    || path.key_copy_position().is_some_and(|(d, _)| d == writer)
                    || path.write_id().is_some_and(|w| w.device == writer)
            }
            _ => false,
        })
        .count()
}

impl Removed {
    /// Membership discovery after a clock observation at storage second
    /// `time`, then write discovery, as one pass does them. The return
    /// checkpoint makes write misses trusted (§15). Returns this pass's requests.
    async fn pass(&mut self, time: u64) -> Result<Vec<MemoryRequest>, SyncError> {
        self.clock.set(second(time));
        let before = self.storage.request_trace().len();
        let _pass = self.ana.sync.begin_pass(&mut self.ana.writes);
        self.ana
            .sync
            .observe_clock(&self.ana.db.local_store_log().await?)
            .await?;
        self.ana.sync.sync_store_log().await?;
        self.ana
            .db
            .save_return_checkpoint(coven_format::value::StorageTime::new(time, 0).unwrap())
            .await?;
        self.ana.writes.download_writes().await?;
        Ok(self.storage.request_trace()[before..].to_vec())
    }

    async fn drained(&self) -> bool {
        self.ana
            .db
            .drained_devices()
            .await
            .unwrap()
            .contains(&DeviceId(2))
    }

    async fn reopen(&mut self) {
        self.ana
            .reopen(
                self.storage.clone(),
                vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                vec![schema::initial()],
            )
            .await;
    }
}

/// Ben's laptop is polled until a sample strictly past F + one day + five
/// minutes precedes all three of its logs' misses. Then Ana marks it drained
/// and stops probing it, also after reopening. F is the sample of the
/// finality observation, not the removal's publication time.
#[tokio::test]
async fn a_removed_device_is_drained_once_after_its_finality_wait() {
    let mut removed = removed().await;
    removed.pass(FINAL - 1).await.unwrap();
    assert!(removed
        .ana
        .db
        .retirement_finalities()
        .await
        .unwrap()
        .is_empty());
    removed.pass(FINAL).await.unwrap();
    let finality = removed.ana.db.retirement_finalities().await.unwrap();
    assert_eq!(finality.len(), 1);
    assert_eq!(
        finality[0].final_at,
        coven_format::value::StorageTime::new(FINAL, 0).unwrap()
    );
    // Restart before the wait keeps F; a sample at exactly F + wait is a miss
    // before the qualifying T.
    removed.reopen().await;
    let trace = removed.pass(FINAL + WAIT).await.unwrap();
    assert!(probes(&trace, DeviceId(2)) >= 3);
    assert!(!removed.drained().await);
    let trace = removed.pass(FINAL + WAIT + 1).await.unwrap();
    assert!(probes(&trace, DeviceId(2)) >= 3);
    assert!(removed.drained().await);
    let trace = removed.pass(FINAL + WAIT + 2).await.unwrap();
    assert_eq!(probes(&trace, DeviceId(2)), 0);
    removed.reopen().await;
    let trace = removed.pass(FINAL + WAIT + 3).await.unwrap();
    assert_eq!(probes(&trace, DeviceId(2)), 0);
    assert!(removed.drained().await);
}

/// A failed read in any of the three logs leaves the drain pending; the next
/// due pass finishes it from fresh misses.
#[tokio::test]
async fn a_failed_drain_read_leaves_the_device_polled() {
    for log in ["entry", "copy", "write"] {
        let mut removed = removed().await;
        removed.pass(FINAL).await.unwrap();
        let mut faults = coven_storage::test_utils::Faults::none();
        match log {
            "entry" => {
                faults.read_failures.insert(
                    ObjectPath::store_log(STORE_ID, DeviceId(2), 1.try_into().unwrap()),
                    StorageFailure::Network,
                );
            }
            "copy" => {
                faults.read_failures.insert(
                    ObjectPath::key_copy(STORE_ID, DeviceId(2), 1.try_into().unwrap()),
                    StorageFailure::Network,
                );
            }
            _ => {
                faults.status_failures.insert(
                    ObjectPath::device_log(STORE_ID, DeviceId(2), 2.try_into().unwrap()),
                    StorageFailure::Network,
                );
            }
        }
        removed.storage.set_faults(faults).await;
        assert!(removed.pass(FINAL + WAIT + 1).await.is_err(), "{log}");
        assert!(!removed.drained().await, "{log}");
        removed
            .storage
            .set_faults(coven_storage::test_utils::Faults::none())
            .await;
        removed.pass(FINAL + WAIT + 2).await.unwrap();
        assert!(removed.drained().await, "{log}");
    }
}

/// A drained writer's discovered but unapplied write is processed from its
/// retained observation, never by probing the next number again.
#[tokio::test]
async fn a_drained_writers_retained_write_still_applies() {
    let mut removed = removed().await;
    let path = ObjectPath::device_log(STORE_ID, DeviceId(2), 1.try_into().unwrap());
    let mut faults = coven_storage::test_utils::Faults::none();
    faults
        .status_failures
        .insert(path.clone(), StorageFailure::Network);
    removed.storage.set_faults(faults).await;
    assert!(removed.pass(FINAL).await.is_err());
    removed
        .storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    assert!(rows(&removed.ana.db).await.is_empty());
    let finality = removed.ana.db.retirement_finalities().await.unwrap()[0];
    // Write 1 was discovered and retained before a failure interrupted it.
    let object = removed.storage.status(&path).await.unwrap().unwrap();
    crate::object_input::ObjectInput::observe(&removed.ana.db, &object)
        .await
        .unwrap();
    assert!(removed
        .ana
        .db
        .mark_drained(
            finality,
            coven_format::value::StorageTime::new(FINAL + WAIT + 1, 0).unwrap()
        )
        .await
        .unwrap());
    let before = removed.storage.request_trace().len();
    removed.ana.writes.download_writes().await.unwrap();
    assert_eq!(rows(&removed.ana.db).await.len(), 1);
    let trace = &removed.storage.request_trace()[before..];
    assert!(trace.iter().all(|request| !matches!(request,
        MemoryRequest::Status(p) if p.write_id().is_some_and(|w| w.device == DeviceId(2)))));
}

/// IO `Retired`: the actual drain prerequisites (the strict wait after F and a
/// terminal miss in each of the three logs, retained across a failure) and the
/// resulting polling set agree with the model.
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_retired_drain() {
    use serde_json::json;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for offset in [0, 1] {
        for failed in [None, Some(0), Some(1), Some(2)] {
            let (drained, polled) = runtime.block_on(async {
                let mut removed = removed().await;
                removed.pass(FINAL).await.unwrap();
                let mut faults = coven_storage::test_utils::Faults::none();
                match failed {
                    Some(0) => {
                        faults.read_failures.insert(
                            ObjectPath::store_log(STORE_ID, DeviceId(2), 1.try_into().unwrap()),
                            StorageFailure::Network,
                        );
                    }
                    Some(1) => {
                        faults.read_failures.insert(
                            ObjectPath::key_copy(STORE_ID, DeviceId(2), 1.try_into().unwrap()),
                            StorageFailure::Network,
                        );
                    }
                    Some(_) => {
                        faults.status_failures.insert(
                            ObjectPath::device_log(STORE_ID, DeviceId(2), 2.try_into().unwrap()),
                            StorageFailure::Network,
                        );
                    }
                    None => {}
                }
                removed.storage.set_faults(faults).await;
                let _ = removed.pass(FINAL + WAIT + offset).await;
                removed
                    .storage
                    .set_faults(coven_storage::test_utils::Faults::none())
                    .await;
                let drained = removed.drained().await;
                let trace = removed.pass(FINAL + WAIT + offset).await;
                // A drained device's logs are not probed by the following pass.
                let mut polled = vec![1];
                if probes(&trace.unwrap(), DeviceId(2)) > 0 {
                    polled.push(2);
                }
                (drained, polled)
            });
            // Device 2 published no entries or copies and one write; Ana
            // has received its write, so each scan starts at that position.
            let logs: Vec<_> = (0..3)
                .map(|kind| {
                    let count = u64::from(kind == 2);
                    json!({"count":count,"start":count,"failed":failed == Some(kind)})
                })
                .collect();
            cases.push(
                json!({"check":"drain","final":FINAL,"sample":FINAL + WAIT + offset,
                "writer":2,"known":[1,2],"logs":logs}),
            );
            results.push(json!({"drained":drained,"polled":polled}));
        }
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
