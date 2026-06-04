//! Tests for outbox upload processing: record-and-continue, per-entry backoff,
//! and the upload lifecycle observer callbacks.
//!
//! These drive the real `process_uploads` against in-memory fakes of its
//! dependencies — a `MockBookkeeping` (the host DB), a `RecordingObserver`, and
//! `InMemoryCloudHome` / `FailingCloudHome` (the cloud backend). The unit under
//! test is `process_uploads` itself; only its dependencies are faked.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::Duration;

use super::outbox::{backoff_window, process_uploads};
use crate::blob::BlobUploadObserver;
use crate::clock::{Clock, FixedClock};
use crate::db::{DbError, OutboxEntry, OutboxOperation, SyncBookkeeping};
use crate::encryption::EncryptionService;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

// --- Fakes -----------------------------------------------------------------

/// In-memory `SyncBookkeeping`: backs `cloud_outbox` with a Vec. Only the
/// outbox methods `process_uploads` calls are implemented.
struct MockBookkeeping {
    entries: Mutex<Vec<OutboxEntry>>,
}

impl MockBookkeeping {
    fn with_uploads(entries: Vec<OutboxEntry>) -> Self {
        Self {
            entries: Mutex::new(entries),
        }
    }

    fn get(&self, id: i64) -> Option<OutboxEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.id == id)
            .cloned()
    }
}

#[async_trait::async_trait]
impl SyncBookkeeping for MockBookkeeping {
    async fn get_sync_state(&self, _key: &str) -> Result<Option<String>, DbError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn max_synced_updated_at(&self) -> Result<Option<String>, DbError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn set_sync_state(&self, _key: &str, _value: &str) -> Result<(), DbError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn get_all_sync_cursors(&self) -> Result<HashMap<String, u64>, DbError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn set_sync_cursor(&self, _device_id: &str, _seq: u64) -> Result<(), DbError> {
        unimplemented!("not exercised by process_uploads")
    }

    async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.operation == OutboxOperation::Upload)
            .cloned()
            .collect())
    }

    async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.operation == OutboxOperation::Delete)
            .cloned()
            .collect())
    }

    async fn has_pending_cloud_uploads(&self) -> Result<bool, DbError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.operation == OutboxOperation::Upload))
    }

    async fn remove_cloud_outbox_entry(&self, id: i64) -> Result<(), DbError> {
        self.entries.lock().unwrap().retain(|e| e.id != id);
        Ok(())
    }

    async fn record_cloud_upload_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(e) = entries.iter_mut().find(|e| e.id == id) {
            e.attempt_count += 1;
            e.last_error = Some(error.to_string());
            e.last_attempt_at = Some(attempted_at.to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ObsEvent {
    Started(String),
    Uploaded(String),
    Failed(String, String),
}

/// Records the upload-lifecycle callbacks in arrival order.
struct RecordingObserver {
    events: Mutex<Vec<ObsEvent>>,
}

impl RecordingObserver {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
    fn events(&self) -> Vec<ObsEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl BlobUploadObserver for RecordingObserver {
    async fn on_blob_upload_started(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Started(file_id.to_string()));
    }
    async fn on_blob_uploaded(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Uploaded(file_id.to_string()));
    }
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Failed(file_id.to_string(), error.to_string()));
    }
}

/// A cloud backend whose `write` always fails. Counts write attempts so a test
/// can assert that a backed-off entry was not attempted. `process_uploads`
/// calls only `write`, so the rest is unreachable.
struct FailingCloudHome {
    write_calls: AtomicUsize,
}

impl FailingCloudHome {
    fn new() -> Self {
        Self {
            write_calls: AtomicUsize::new(0),
        }
    }
    fn write_calls(&self) -> usize {
        self.write_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CloudHome for FailingCloudHome {
    async fn write(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Storage("induced write failure".into()))
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
}

// --- Helpers ---------------------------------------------------------------

const T0: &str = "2024-06-01T00:00:00Z";

fn fixed_clock(rfc3339: &str) -> FixedClock {
    FixedClock(
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&chrono::Utc),
    )
}

fn enc() -> RwLock<EncryptionService> {
    RwLock::new(EncryptionService::new_with_key(&[0u8; 32]))
}

fn upload_entry(
    id: i64,
    file_id: &str,
    cloud_key: &str,
    source_path: Option<String>,
) -> OutboxEntry {
    OutboxEntry {
        id,
        operation: OutboxOperation::Upload,
        file_id: file_id.to_string(),
        cloud_key: cloud_key.to_string(),
        source_path,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        min_seq: None,
        attempt_count: 0,
        last_error: None,
        last_attempt_at: None,
    }
}

fn write_temp_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().to_string()
}

// --- Tests -----------------------------------------------------------------

/// Record-and-continue: a failing entry no longer stops the drain, so a good
/// entry queued behind it still uploads in the same cycle.
#[tokio::test]
async fn bad_item_does_not_block_good_later_item() {
    let tmp = tempfile::tempdir().unwrap();
    let good_path = write_temp_file(tmp.path(), "good.bin", b"good-bytes");
    let missing_path = tmp.path().join("missing.bin").to_string_lossy().to_string();

    let db = MockBookkeeping::with_uploads(vec![
        upload_entry(1, "fa", "key-a", Some(missing_path)), // read fails
        upload_entry(2, "fb", "key-b", Some(good_path)),    // uploads fine
    ]);
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();
    let clock = fixed_clock(T0);

    let n = process_uploads(&db, &cloud, &enc(), tmp.path(), &clock, Some(&observer))
        .await
        .unwrap();

    assert_eq!(n, 1, "the good entry uploads despite the earlier failure");
    assert!(cloud.get("key-b").is_some(), "good blob landed in cloud");
    assert!(cloud.get("key-a").is_none(), "failed blob did not land");

    let a = db.get(1).expect("failed entry stays queued");
    assert_eq!(a.attempt_count, 1);
    assert!(a.last_error.is_some());
    let recorded =
        chrono::DateTime::parse_from_rfc3339(a.last_attempt_at.as_deref().unwrap()).unwrap();
    assert_eq!(recorded.with_timezone(&chrono::Utc), clock.now());

    assert!(db.get(2).is_none(), "uploaded entry removed");
}

/// A failed attempt persists attempt_count + last_error, and a later cycle past
/// the backoff window retries and bumps the count again.
#[tokio::test]
async fn failure_persists_attempt_count_and_last_error() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = MockBookkeeping::with_uploads(vec![upload_entry(1, "f1", "k1", Some(path))]);
    let cloud = FailingCloudHome::new();

    process_uploads(&db, &cloud, &enc(), tmp.path(), &fixed_clock(T0), None)
        .await
        .unwrap();
    let e = db.get(1).unwrap();
    assert_eq!(e.attempt_count, 1);
    assert!(e
        .last_error
        .as_deref()
        .unwrap()
        .contains("cloud write failed"));

    // 31s later — past the 30s window for attempt_count==1 → retried.
    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:31Z"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(db.get(1).unwrap().attempt_count, 2);
    assert_eq!(cloud.write_calls(), 2);
}

/// An entry still inside its backoff window is skipped — not read, not written,
/// no started event, attempt_count untouched — then retried once the window
/// elapses.
#[tokio::test]
async fn backoff_skips_item_inside_window() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let mut entry = upload_entry(1, "f1", "k1", Some(path));
    entry.attempt_count = 1;
    entry.last_attempt_at = Some(T0.to_string());
    let db = MockBookkeeping::with_uploads(vec![entry]);
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    // 10s after last attempt: inside the 30s window for attempt_count==1.
    let n = process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:10Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        cloud.write_calls(),
        0,
        "no write attempted inside backoff window"
    );
    assert!(
        observer.events().is_empty(),
        "no started event for skipped entry"
    );
    assert_eq!(
        db.get(1).unwrap().attempt_count,
        1,
        "attempt_count unchanged"
    );

    // 31s after last attempt: window elapsed → attempted (and fails again).
    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:31Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(
        cloud.write_calls(),
        1,
        "write attempted past backoff window"
    );
    assert_eq!(db.get(1).unwrap().attempt_count, 2);
}

#[tokio::test]
async fn observer_fires_started_then_uploaded_on_success() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = MockBookkeeping::with_uploads(vec![upload_entry(1, "fid", "k1", Some(path))]);
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();

    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    assert_eq!(
        observer.events(),
        vec![
            ObsEvent::Started("fid".into()),
            ObsEvent::Uploaded("fid".into())
        ]
    );
}

#[tokio::test]
async fn observer_fires_started_then_failed_on_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = MockBookkeeping::with_uploads(vec![upload_entry(1, "fid", "k1", Some(path))]);
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    let events = observer.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], ObsEvent::Started("fid".into()));
    match &events[1] {
        ObsEvent::Failed(fid, err) => {
            assert_eq!(fid, "fid");
            assert!(err.contains("cloud write failed"));
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn backoff_window_is_exponential_and_capped() {
    assert_eq!(backoff_window(0), Duration::zero());
    assert_eq!(backoff_window(1), Duration::seconds(30));
    assert_eq!(backoff_window(2), Duration::seconds(60));
    assert_eq!(backoff_window(3), Duration::seconds(120));
    // 30 · 2^7 = 3840, capped to the 3600s ceiling.
    assert_eq!(backoff_window(8), Duration::seconds(3600));
    assert_eq!(backoff_window(50), Duration::seconds(3600));
}
