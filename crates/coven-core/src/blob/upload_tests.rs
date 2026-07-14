//! Tests for the blob engine's upload drain: record-and-continue, per-entry
//! backoff, scope-resolved sealing, and the upload lifecycle observer callbacks.
//!
//! These drive the real [`drain_uploads`] against a real [`crate::database::Database`]
//! (carrying the `cloud_outbox` bookkeeping table), a `RecordingObserver`, and
//! `InMemoryCloudHome` / `FailingCloudHome` (the cloud backend). The unit under
//! test is `drain_uploads` itself; only the cloud backend and observer are fakes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::Duration;

use super::upload::{backoff_window, drain_uploads, DrainOutcome};
use crate::blob::BlobTransitionObserver;
use crate::clock::{Clock, FixedClock};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
use crate::sync::hlc::Hlc;
use rusqlite::OptionalExtension;

/// Run the real [`drain_uploads`] with a throwaway HLC, the register coven stamps a
/// manage flip from. These drain tests carry no synced/gated tables (an `open_outbox_
/// db` has only the bookkeeping schema), so no upload resolves to a gated root — the
/// completion flip never fires and the HLC only ever mints the stamps that go unused.
async fn run_drain(
    db: &Database,
    cloud: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    store_dir: &StoreDir,
    clock: &dyn Clock,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<DrainOutcome, crate::database::DbError> {
    let hlc = Hlc::new("test-device".to_string());
    drain_uploads(
        db,
        cloud,
        cipher,
        &PendingRotation::none(),
        "test-lib",
        store_dir,
        clock,
        &hlc,
        observer,
    )
    .await
}

// --- Database under test ----------------------------------------------------

/// A `Database` over an in-memory connection with just the bookkeeping tables —
/// no synced tables (the upload drain doesn't need them). The upload queue lives
/// in coven's `cloud_outbox` migration table, created by `Database::open`.
fn open_outbox_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        "test-device".to_string(),
        &[],
    )
    .expect("open outbox database");
    db
}

/// An outbox-only `Database` whose upload drain runs up to `uploads` writes at once
/// (downloads stay serial — not exercised by the drain).
fn open_outbox_db_with_uploads(uploads: usize) -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits {
            uploads: std::num::NonZeroUsize::new(uploads).expect("uploads limit is nonzero"),
            downloads: std::num::NonZeroUsize::MIN,
        },
        "test-device".to_string(),
        &[],
    )
    .expect("open outbox database");
    db
}

/// Insert a fully-specified `cloud_outbox` upload row.
async fn insert_upload(
    db: &Database,
    id: i64,
    file_id: &str,
    cloud_key: &str,
    source_path: Option<String>,
    attempt_count: i64,
    last_attempt_at: Option<String>,
) {
    let expected_hash = source_path
        .as_deref()
        .and_then(|path| std::fs::read(path).ok())
        .map(|bytes| crate::blob::content_hash(&bytes))
        .unwrap_or_else(|| crate::blob::content_hash(b"missing-source"));
    let (file_id, cloud_key) = (file_id.to_string(), cloud_key.to_string());
    let scope = crate::blob::BlobScope::Master.to_outbox_str();
    db.call(move |conn| {
        conn.execute(
            &format!(
                "INSERT INTO cloud_outbox \
                 (id, operation, file_id, cloud_key, source_path, expected_hash, scope, created_at, \
                  attempt_count, last_attempt_at) \
                 VALUES (?1, 'upload', ?2, ?3, ?4, ?5, '{scope}', '2024-01-01T00:00:00Z', ?6, ?7)"
            ),
            rusqlite::params![
                id,
                file_id,
                cloud_key,
                source_path,
                expected_hash,
                attempt_count,
                last_attempt_at
            ],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("insert outbox upload");
}

/// Read back `(attempt_count, last_error, last_attempt_at)` for an entry, or
/// `None` if it was removed.
async fn get_upload(db: &Database, id: i64) -> Option<(i64, Option<String>, Option<String>)> {
    db.call(move |conn| {
        conn.query_row(
            "SELECT attempt_count, last_error, last_attempt_at FROM cloud_outbox WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(DbError::from)
    })
    .await
    .expect("query outbox entry")
}

// --- Fakes ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum ObsEvent {
    Started(String),
    Progress(String, u64, u64),
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobTransitionObserver for RecordingObserver {
    async fn on_blob_upload_started(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Started(file_id.to_string()));
    }
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {
        self.events.lock().unwrap().push(ObsEvent::Progress(
            file_id.to_string(),
            bytes_done,
            bytes_total,
        ));
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
/// can assert that a backed-off entry was not attempted.
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CloudHome for FailingCloudHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Transport("induced write failure".into()))
    }
    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Transport("induced write failure".into()))
    }
    fn multipart_threshold(&self) -> u64 {
        // Small upload payloads in these tests go via put_object.
        8 * 1024 * 1024
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
}

/// A cloud backend whose multipart sink accepts each part after a delay, so the
/// upload spans several of `drain_uploads`' coalescing ticks and the driver's
/// per-part progress advances over time.
struct SlowChunkedCloudHome {
    chunk: usize,
    per_chunk_delay: std::time::Duration,
}

/// The delay sink: each `send_part` sleeps, so the driver's progress advances one
/// part at a time across several ticks.
struct SlowPartSink {
    part_size: usize,
    per_chunk_delay: std::time::Duration,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl crate::storage::cloud::PartSink for SlowPartSink {
    fn part_size(&self) -> usize {
        self.part_size
    }
    async fn send_part(
        &mut self,
        _part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        tokio::time::sleep(self.per_chunk_delay).await;
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CloudHome for SlowChunkedCloudHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        Ok(())
    }
    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        Ok(Box::new(SlowPartSink {
            part_size: self.chunk,
            per_chunk_delay: self.per_chunk_delay,
        }))
    }
    fn multipart_threshold(&self) -> u64 {
        // Stream every non-empty payload so the delay sink drives progress.
        0
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
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

fn enc() -> RwLock<CloudCipher> {
    RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [0u8; 32],
    )))
}

fn write_temp_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().to_string()
}

// --- Tests -----------------------------------------------------------------

#[tokio::test]
async fn bad_item_does_not_block_good_later_item() {
    let tmp = tempfile::tempdir().unwrap();
    let good_path = write_temp_file(tmp.path(), "good.bin", b"good-bytes");
    let missing_path = tmp.path().join("missing.bin").to_string_lossy().to_string();

    let db = open_outbox_db();
    insert_upload(&db, 1, "fa", "key-a", Some(missing_path), 0, None).await; // read fails
    insert_upload(&db, 2, "fb", "key-b", Some(good_path), 0, None).await; // uploads fine
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();
    let clock = fixed_clock(T0);

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &clock,
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;

    assert_eq!(n, 1, "the good entry uploads despite the earlier failure");
    assert!(cloud.get("key-b").is_some(), "good blob landed in cloud");
    assert!(cloud.get("key-a").is_none(), "failed blob did not land");

    let (attempt, err, last) = get_upload(&db, 1).await.expect("failed entry stays queued");
    assert_eq!(attempt, 1);
    assert!(err.is_some());
    let recorded = chrono::DateTime::parse_from_rfc3339(last.as_deref().unwrap()).unwrap();
    assert_eq!(recorded.with_timezone(&chrono::Utc), clock.now());

    assert!(get_upload(&db, 2).await.is_none(), "uploaded entry removed");
}

/// While this device has not adopted a store-key rotation the cloud has already
/// committed, the drain refuses to seal any entry rather than sealing it under
/// the superseded generation: no object reaches the cloud, and the entry stays
/// queued (the same "recorded and skipped" shape a cloud write failure takes),
/// to retry once adoption clears the marker.
#[tokio::test]
async fn upload_refuses_to_seal_while_a_rotation_is_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "f1", "k1", Some(path), 0, None).await;
    let cloud = InMemoryCloudHome::new();
    let cipher = enc();
    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2);

    let outcome = drain_uploads(
        &db,
        &cloud,
        &cipher,
        &pending_rotation,
        "test-lib",
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        &Hlc::new("test-device".to_string()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.uploaded, 0,
        "nothing seals while adoption is pending"
    );
    assert!(cloud.get("k1").is_none(), "no object reaches the cloud");
    let (attempt, err, _) = get_upload(&db, 1).await.expect("entry stays queued");
    assert_eq!(attempt, 1);
    assert!(
        err.as_deref().unwrap().contains("rotated to generation 2"),
        "the recorded failure names the rotation, got {err:?}"
    );
}

/// A failed attempt persists attempt_count + last_error, and a later cycle past
/// the backoff window retries and bumps the count again.
#[tokio::test]
async fn failure_persists_attempt_count_and_last_error() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "f1", "k1", Some(path), 0, None).await;
    let cloud = FailingCloudHome::new();

    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await
    .unwrap();
    let (attempt, err, _) = get_upload(&db, 1).await.unwrap();
    assert_eq!(attempt, 1);
    assert!(err.as_deref().unwrap().contains("cloud write failed"));

    // 31s later — past the 30s window for attempt_count==1 → retried.
    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock("2024-06-01T00:00:31Z"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(get_upload(&db, 1).await.unwrap().0, 2);
    assert_eq!(cloud.write_calls(), 2);
}

/// An entry still inside its backoff window is skipped — not read, not written,
/// no started event, attempt_count untouched — then retried once the window
/// elapses.
#[tokio::test]
async fn backoff_skips_item_inside_window() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "f1", "k1", Some(path), 1, Some(T0.to_string())).await;
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    // 10s after last attempt: inside the 30s window for attempt_count==1.
    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock("2024-06-01T00:00:10Z"),
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;
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
        get_upload(&db, 1).await.unwrap().0,
        1,
        "attempt_count unchanged"
    );

    // 31s after last attempt: window elapsed → attempted (and fails again).
    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
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
    assert_eq!(get_upload(&db, 1).await.unwrap().0, 2);
}

#[tokio::test]
async fn malformed_retry_timestamp_fails_drain_without_cloud_write() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(
        &db,
        1,
        "fid",
        "k1",
        Some(path),
        1,
        Some("not-a-timestamp".to_string()),
    )
    .await;
    let cloud = InMemoryCloudHome::new();

    let result = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await;
    let error = match result {
        Ok(_) => panic!("malformed durable retry timestamp must fail the drain"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("last_attempt_at"),
        "error names the malformed durable field: {error}",
    );
    assert!(cloud.get("k1").is_none(), "no cloud write is attempted");
    assert!(
        get_upload(&db, 1).await.is_some(),
        "the queue row is retained"
    );
}

#[tokio::test]
async fn observer_fires_started_then_uploaded_on_success() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();

    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    // A small file uploads instantly, so the coalescing ticker never fires; the
    // terminal progress forward on success still emits one full-size Progress
    // between Started and Uploaded.
    let events = observer.events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], ObsEvent::Started("fid".into()));
    assert_eq!(events[2], ObsEvent::Uploaded("fid".into()));
    match events[1] {
        ObsEvent::Progress(ref fid, done, total) => {
            assert_eq!(fid, "fid");
            assert_eq!(done, total, "terminal forward reports done == total");
            assert!(total > 5, "encrypted size exceeds the 5 plaintext bytes");
        }
        ref other => panic!("expected Progress, got {other:?}"),
    }
}

#[tokio::test]
async fn observer_fires_started_then_failed_on_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
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

#[tokio::test]
async fn upload_failure_recording_failure_fails_the_drain() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    db.call(|conn| {
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_upload_failure_record \
             BEFORE UPDATE OF attempt_count, last_error, last_attempt_at ON cloud_outbox BEGIN \
             SELECT RAISE(ABORT, 'forced upload failure recording failure'); END;",
        )
        .map_err(DbError::from)
    })
    .await
    .expect("install upload failure recording trigger");
    let observer = RecordingObserver::new();

    let result = run_drain(
        &db,
        &FailingCloudHome::new(),
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("failure-recording error must fail the drain"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("forced upload failure recording failure"),
        "the drain surfaces the bookkeeping failure: {error}",
    );
    assert_eq!(
        get_upload(&db, 1).await.unwrap().0,
        0,
        "the failed update leaves the original queue row intact",
    );
    assert!(
        !observer
            .events()
            .iter()
            .any(|event| matches!(event, ObsEvent::Failed(_, _))),
        "a failure that was not durably recorded cannot be notified",
    );
}

#[tokio::test]
async fn post_upload_commit_failure_fails_the_drain_without_reporting_success() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    db.call(|conn| {
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_upload_finish \
             BEFORE DELETE ON cloud_outbox WHEN OLD.id = 1 BEGIN \
             SELECT RAISE(ABORT, 'forced post-upload commit failure'); END;",
        )
        .map_err(DbError::from)
    })
    .await
    .expect("install post-upload commit failure trigger");
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();

    let result = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("post-upload commit failure must fail the drain"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("forced post-upload commit failure"),
        "the drain surfaces the commit failure: {error}",
    );
    assert!(
        cloud.get("k1").is_some(),
        "the idempotent cloud write landed"
    );
    assert!(
        get_upload(&db, 1).await.is_some(),
        "the atomic commit leaves the upload queued",
    );
    assert!(
        !observer
            .events()
            .iter()
            .any(|event| matches!(event, ObsEvent::Uploaded(_))),
        "a failed commit cannot notify upload success",
    );
}

/// A slow, chunked upload reports mid-file progress: the coalescing ticker
/// forwards an advancing byte count to the observer between Started and Uploaded,
/// and the final forwarded value equals the total.
#[tokio::test(start_paused = true)]
async fn observer_receives_advancing_midfile_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let total = 10_000usize;
    let path = write_temp_file(tmp.path(), "big.bin", &vec![7u8; total]);
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = SlowChunkedCloudHome {
        chunk: 1000,
        per_chunk_delay: std::time::Duration::from_millis(500),
    };
    let observer = RecordingObserver::new();

    run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    let events = observer.events();
    assert_eq!(events.first(), Some(&ObsEvent::Started("fid".into())));
    assert_eq!(events.last(), Some(&ObsEvent::Uploaded("fid".into())));

    let progress: Vec<(u64, u64)> = events
        .iter()
        .filter_map(|e| match e {
            ObsEvent::Progress(fid, done, total) if fid == "fid" => Some((*done, *total)),
            _ => None,
        })
        .collect();
    assert!(
        progress.len() >= 2,
        "expected several mid-file progress ticks, got {progress:?}"
    );
    for w in progress.windows(2) {
        assert!(w[1].0 >= w[0].0, "progress went backwards: {progress:?}");
    }
    let (last_done, last_total) = *progress.last().unwrap();
    assert_eq!(
        last_done, last_total,
        "final progress reports done == total"
    );
    assert!(last_total >= total as u64, "total covers the whole payload");
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

/// `enqueue_upload_on` composes with a host transaction: a rollback takes the
/// queued upload with it, and a commit lands it — so a host can make "row +
/// its upload intent" a single atomic fact.
#[tokio::test]
async fn enqueue_upload_on_is_transactional_with_host_writes() {
    let db = open_outbox_db();

    // Rolled-back host transaction: the enqueue must vanish with it.
    db.call(|conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        Database::enqueue_upload_on(
            &tx,
            "f-rollback",
            "k-rollback",
            None,
            crate::blob::BlobScope::Master,
            false,
            &crate::blob::content_hash(b"rollback"),
            T0,
        )?;
        tx.rollback().map_err(DbError::from)
    })
    .await
    .expect("rolled-back transaction");

    // Committed host transaction: the enqueue lands.
    db.call(|conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        Database::enqueue_upload_on(
            &tx,
            "f-commit",
            "k-commit",
            Some("/tmp/source.flac"),
            crate::blob::BlobScope::Derived("rel-1".to_string()),
            false,
            &crate::blob::content_hash(b"commit"),
            T0,
        )?;
        tx.commit().map_err(DbError::from)
    })
    .await
    .expect("committed transaction");

    let pending = db.get_pending_cloud_uploads().await.expect("pending");
    let keys: Vec<&str> = pending.iter().map(|e| e.cloud_key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["k-commit"],
        "only the committed enqueue persists"
    );
}

/// A `retain_pinned` upload populates the PROTECTED cache folder from the
/// plaintext: after the drain, `storage/pinned/<namespace>/<id>/<content_hash>` holds the plaintext bytes
/// (not the sealed ciphertext the cloud holds), and the evictable
/// `storage/cache/<namespace>/<id>/<content_hash>` is untouched — the blob is kept local and budget-exempt
/// with no later cloud round-trip.
#[tokio::test]
async fn pinned_upload_populates_the_protected_cache_folder() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"PINNED-AUDIO-BYTES";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = StoreDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "pinaaaa1";
    let namespace = "release_files";
    let content_hash = crate::blob::content_hash(plaintext);
    // The cache namespace is derived from the cloud key's first component, so it must
    // be the namespace the assertions below check.
    let cloud_key = "release_files/pi/na/pinaaaa1";
    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        true, // retain_pinned
        &content_hash,
        T0,
    )
    .await
    .expect("enqueue a pinned upload");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("drain")
        .uploaded;
    assert_eq!(n, 1, "the pinned blob uploads");

    // The cloud holds the sealed (encrypted) bytes, not the plaintext.
    let at_rest = cloud.get(cloud_key).expect("blob present in cloud");
    assert_ne!(at_rest, plaintext, "the cloud copy is sealed ciphertext");

    // The protected folder holds the PLAINTEXT (what the cache serves), written
    // straight from the drain's read — no cloud round-trip.
    let pinned_path = ld
        .pinned_blob_path(namespace, file_id, &content_hash)
        .unwrap();
    assert!(
        pinned_path.exists(),
        "a pinned upload writes storage/pinned/<namespace>/<id>/<content_hash>",
    );
    assert_eq!(
        std::fs::read(&pinned_path).unwrap(),
        plaintext,
        "the pinned file is the plaintext, not the sealed cloud bytes",
    );

    // The evictable cache is untouched: a pin populates pinned/, never cache/.
    assert!(
        !ld.cache_blob_path(namespace, file_id, &content_hash)
            .unwrap()
            .exists(),
        "a pinned upload does not populate the evictable storage/cache/<namespace>/<id>/<content_hash>",
    );
}

/// An unpinned upload populates NOTHING on write: after the drain the blob is in
/// the cloud but neither cache folder holds it — the evictable `storage/cache/<namespace>/<id>/<content_hash>`
/// fills only on a later read-miss, never on the upload itself.
#[tokio::test]
async fn unpinned_upload_populates_nothing_on_write() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"UNPINNED-AUDIO-BYTES";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = StoreDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "unpaaaa1";
    let namespace = "release_files";
    let content_hash = crate::blob::content_hash(plaintext);
    // The cache namespace is derived from the cloud key's first component.
    let cloud_key = "release_files/un/pa/unpaaaa1";
    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        false, // retain_pinned
        &content_hash,
        T0,
    )
    .await
    .expect("enqueue an unpinned upload");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("drain")
        .uploaded;
    assert_eq!(n, 1, "the unpinned blob uploads");
    assert!(cloud.get(cloud_key).is_some(), "the blob is in the cloud");

    // Neither folder holds it: an unpinned upload writes no local cache copy.
    assert!(
        !ld.pinned_blob_path(namespace, file_id, &content_hash)
            .unwrap()
            .exists(),
        "an unpinned upload does not populate storage/pinned/<namespace>/<id>/<content_hash>",
    );
    assert!(
        !ld.cache_blob_path(namespace, file_id, &content_hash)
            .unwrap()
            .exists(),
        "an unpinned upload does not populate storage/cache/<namespace>/<id>/<content_hash>",
    );
}

/// A pin populate failure keeps the operation queued and records the failure. The
/// cloud write is idempotent, so retrying the complete operation cannot lose data.
/// Here the protected folder is
/// blocked by planting a FILE where the blob's shard directory must go, so the
/// atomic write into `storage/pinned/<namespace>/<id>/<content_hash>` can't create its parent — yet the drain
/// reports the upload incomplete and retains the queue entry.
#[tokio::test]
async fn a_failed_pin_populate_keeps_the_upload_queued() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"PIN-FAILS-BUT-UPLOAD-OK";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = StoreDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "pinfail1";
    let namespace = "release_files";
    let content_hash = crate::blob::content_hash(plaintext);
    // The cache namespace is derived from the cloud key's first component.
    let cloud_key = "release_files/pi/nf/pinfail1";

    // Block the populate: the pinned blob path is
    // storage/pinned/<namespace>/{ab}/{cd}/<id>; plant a regular FILE at the {ab}
    // level so creating the {ab}/{cd} shard directory fails, and with it the atomic
    // write into pinned/.
    let pinned_path = ld
        .pinned_blob_path(namespace, file_id, &content_hash)
        .unwrap();
    let ab_dir = pinned_path.parent().unwrap().parent().unwrap(); // .../pinned/<namespace>/{ab}
    std::fs::create_dir_all(ab_dir.parent().unwrap()).unwrap(); // .../pinned/<namespace>
    std::fs::write(ab_dir, b"blocker").unwrap(); // {ab} is now a file, not a dir

    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        true, // retain_pinned — but the populate will fail
        &content_hash,
        T0,
    )
    .await
    .expect("enqueue a pinned upload whose populate will fail");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("the drain records the populate failure")
        .uploaded;

    assert_eq!(n, 0, "the operation is not reported complete");
    assert!(cloud.get(cloud_key).is_some(), "the blob reached the cloud");
    assert!(
        get_upload(&db, 1).await.is_some(),
        "the upload remains queued until its pinned copy is durable",
    );
    // The pin did not land (its parent couldn't be created).
    assert!(
        !pinned_path.exists(),
        "the blocked populate left no storage/pinned/<namespace>/<id>/<content_hash> file",
    );
}

// --- bounded concurrency ----------------------------------------------------

/// A cloud backend that gathers writes on a barrier before serving, so a test can
/// prove the drain runs uploads concurrently and bounds them: with a barrier of size
/// N, N writes must arrive together to release it, and `max_inflight` records the
/// observed peak. Records each written key so the test can assert what landed.
struct BarrierCloudHome {
    keys: Mutex<Vec<String>>,
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    barrier: tokio::sync::Barrier,
}

impl BarrierCloudHome {
    fn new(gather: usize) -> Self {
        Self {
            keys: Mutex::new(Vec::new()),
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            barrier: tokio::sync::Barrier::new(gather),
        }
    }
    fn max_inflight(&self) -> usize {
        self.max_inflight.load(Ordering::SeqCst)
    }
    fn keys(&self) -> Vec<String> {
        let mut k = self.keys.lock().unwrap().clone();
        k.sort();
        k
    }
    async fn gather(&self, key: &str) {
        let n = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_inflight.fetch_max(n, Ordering::SeqCst);
        self.barrier.wait().await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        self.keys.lock().unwrap().push(key.to_string());
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CloudHome for BarrierCloudHome {
    async fn put_object(&self, key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.gather(key).await;
        Ok(())
    }
    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        unimplemented!("the probe keeps blobs under the multipart threshold")
    }
    fn multipart_threshold(&self) -> u64 {
        8 * 1024 * 1024
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
}

/// An observer that pauses the drain after its first `admit_before` admission checks:
/// `should_skip_uploads` returns false for the first `admit_before` calls, then true.
/// Records the started blob ids so a test can assert which entries were admitted.
struct PausingObserver {
    admit_before: usize,
    checks: AtomicUsize,
    started: Mutex<Vec<String>>,
}

impl PausingObserver {
    fn new(admit_before: usize) -> Self {
        Self {
            admit_before,
            checks: AtomicUsize::new(0),
            started: Mutex::new(Vec::new()),
        }
    }
    fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobTransitionObserver for PausingObserver {
    async fn on_blob_upload_started(&self, file_id: &str) {
        self.started.lock().unwrap().push(file_id.to_string());
    }
    async fn on_blob_uploaded(&self, _file_id: &str) {}
    async fn on_blob_upload_failed(&self, _file_id: &str, _error: &str) {}
    fn should_skip_uploads(&self) -> bool {
        self.checks.fetch_add(1, Ordering::SeqCst) >= self.admit_before
    }
}

/// Enqueue `n` ready uploads with distinct ids/keys over real temp files, returning
/// the (file_id, cloud_key) pairs in order.
async fn seed_uploads(db: &Database, dir: &std::path::Path, n: usize) -> Vec<(String, String)> {
    let mut ids = Vec::new();
    for i in 0..n {
        let file_id = format!("f{i}");
        let cloud_key = format!("k{i}");
        let path = write_temp_file(
            dir,
            &format!("blob-{i}.bin"),
            format!("bytes-{i}").as_bytes(),
        );
        insert_upload(db, i as i64 + 1, &file_id, &cloud_key, Some(path), 0, None).await;
        ids.push((file_id, cloud_key));
    }
    ids
}

/// At limit 1 the drain is serial: it uploads every entry in queue order, one at a
/// time — each entry's `Started` is immediately followed by its `Uploaded` before the
/// next entry starts — and clears each row.
#[tokio::test]
async fn limit_one_drains_every_entry_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open_outbox_db_with_uploads(1);
    let ids = seed_uploads(&db, tmp.path(), 3).await;
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;
    assert_eq!(n, 3, "every entry uploads");

    for (i, (_file_id, key)) in ids.iter().enumerate() {
        assert!(cloud.get(key).is_some(), "{key} landed in the cloud");
        assert!(
            get_upload(&db, i as i64 + 1).await.is_none(),
            "the uploaded entry's row was removed",
        );
    }

    // Serial order: Sf0,Uf0,Sf1,Uf1,Sf2,Uf2 — no entry starts before the previous
    // one's upload completes.
    let seq: Vec<String> = observer
        .events()
        .iter()
        .filter_map(|e| match e {
            ObsEvent::Started(f) => Some(format!("S{f}")),
            ObsEvent::Uploaded(f) => Some(format!("U{f}")),
            _ => None,
        })
        .collect();
    assert_eq!(
        seq,
        vec!["Sf0", "Uf0", "Sf1", "Uf1", "Sf2", "Uf2"],
        "limit 1 uploads strictly one entry at a time in queue order",
    );
}

/// At limit 2 the drain runs two uploads at once and no more: a barrier that only
/// releases when two writes gather proves both the concurrency and the bound (a limit
/// of 1 would deadlock it). Every entry lands and its row is cleared.
#[tokio::test]
async fn concurrent_drain_overlaps_up_to_the_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open_outbox_db_with_uploads(2);
    let ids = seed_uploads(&db, tmp.path(), 4).await;
    let cloud = BarrierCloudHome::new(2);

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await
    .unwrap()
    .uploaded;

    assert_eq!(n, 4, "every entry uploads");
    assert_eq!(cloud.max_inflight(), 2, "exactly two uploads ran at once");
    let want: Vec<String> = ids.iter().map(|(_, k)| k.clone()).collect();
    assert_eq!(cloud.keys(), want, "every blob reached the cloud");
    for i in 0..ids.len() {
        assert!(
            get_upload(&db, i as i64 + 1).await.is_none(),
            "every uploaded entry's row was removed after the concurrent batch",
        );
    }
}

/// A single blob's failure is isolated under concurrency: the drain records it and
/// keeps the failed entry queued, while every other blob uploads and clears.
#[tokio::test]
async fn concurrent_drain_isolates_a_failed_upload() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open_outbox_db_with_uploads(3);
    // Entry 2's source file is missing, so its read fails; 1 and 3 upload fine.
    let good_a = write_temp_file(tmp.path(), "a.bin", b"aaa");
    let missing = tmp.path().join("missing.bin").to_string_lossy().to_string();
    let good_c = write_temp_file(tmp.path(), "c.bin", b"ccc");
    insert_upload(&db, 1, "fa", "ka", Some(good_a), 0, None).await;
    insert_upload(&db, 2, "fb", "kb", Some(missing), 0, None).await;
    insert_upload(&db, 3, "fc", "kc", Some(good_c), 0, None).await;
    let cloud = InMemoryCloudHome::new();

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await
    .unwrap()
    .uploaded;

    assert_eq!(n, 2, "the two good blobs upload despite the failure");
    assert!(cloud.get("ka").is_some());
    assert!(cloud.get("kc").is_some());
    assert!(cloud.get("kb").is_none(), "the failed blob did not land");
    assert!(get_upload(&db, 1).await.is_none(), "good entry cleared");
    assert!(get_upload(&db, 3).await.is_none(), "good entry cleared");
    let (attempt, err, _) = get_upload(&db, 2).await.expect("failed entry stays queued");
    assert_eq!(attempt, 1);
    assert!(err.is_some());
}

/// A queue paused up front admits nothing under concurrency: no write, no started
/// event, every row left queued.
#[tokio::test]
async fn paused_queue_admits_nothing_under_concurrency() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open_outbox_db_with_uploads(3);
    seed_uploads(&db, tmp.path(), 3).await;
    let cloud = InMemoryCloudHome::new();
    let observer = PausingObserver::new(0); // pause before the first admission

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;

    assert_eq!(n, 0, "a paused queue uploads nothing");
    assert!(cloud.is_empty(), "no object reached the cloud");
    assert!(observer.started().is_empty(), "no upload started");
    for i in 1..=3 {
        assert!(get_upload(&db, i).await.is_some(), "every row stays queued");
    }
}

/// A pause that trips after the first admission (limit 1) lets the in-flight upload
/// finish and stops admitting the rest: the first entry uploads and clears, the
/// second is untouched.
#[tokio::test]
async fn pause_after_first_finishes_inflight_and_stops_admitting() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open_outbox_db_with_uploads(1);
    seed_uploads(&db, tmp.path(), 2).await;
    let cloud = InMemoryCloudHome::new();
    let observer = PausingObserver::new(1); // admit one, then pause

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &StoreDir::new(tmp.path()),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;

    assert_eq!(
        n, 1,
        "the first entry uploads before the pause takes effect"
    );
    assert_eq!(
        observer.started(),
        vec!["f0".to_string()],
        "only one started"
    );
    assert!(cloud.get("k0").is_some(), "the admitted blob landed");
    assert!(
        cloud.get("k1").is_none(),
        "the paused-out blob did not land"
    );
    assert!(
        get_upload(&db, 1).await.is_none(),
        "the uploaded entry cleared"
    );
    assert!(
        get_upload(&db, 2).await.is_some(),
        "the paused-out entry stays queued",
    );
}
