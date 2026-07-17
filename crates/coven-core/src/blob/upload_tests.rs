//! Tests for the exact blob upload journal and drain.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Duration;
use rusqlite::OptionalExtension;

use super::upload::{backoff_window, drain_uploads, DrainOutcome};
use crate::blob::{BlobTransitionObserver, CacheFill, Provenance};
use crate::clock::{Clock, FixedClock};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{
    BlobBody, BlobBody as ExactBlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudFileReadError, CloudHeadCreateError, CloudHeadReplaceError, CloudHeadStorage,
    CloudHeadVersion, CloudHome, CloudHomeError, CloudHomeJoinInfo, CloudVersionedHead,
    ExactSlotStorage, ObjectSlot, RevokeOutcome, UploadProgress,
};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::hlc::Hlc;
use crate::sync::session::BlobDecl;
use crate::sync::test_helpers::{
    create_exact_test_store, test_migrations, test_synced_tables_with_blob,
};

const T0: &str = "2024-06-01T00:00:00Z";
const ROOT_ID: &str = "upload-root";

fn fixed_clock(rfc3339: &str) -> FixedClock {
    FixedClock(
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&chrono::Utc),
    )
}

#[derive(Clone)]
struct InstrumentedHome {
    inner: InMemoryCloudHome,
    fail_creates: Arc<AtomicBool>,
    create_calls: Arc<AtomicUsize>,
    keys: Arc<Mutex<Vec<String>>>,
    inflight: Arc<AtomicUsize>,
    max_inflight: Arc<AtomicUsize>,
    barrier: Option<Arc<tokio::sync::Barrier>>,
    barrier_enabled: Arc<AtomicBool>,
    slow_chunk: Arc<AtomicUsize>,
    slow_delay_ms: Arc<AtomicU64>,
}

impl InstrumentedHome {
    fn new() -> Self {
        Self::with_barrier(None)
    }

    fn with_barrier(gather: Option<usize>) -> Self {
        Self {
            inner: InMemoryCloudHome::new(),
            fail_creates: Arc::new(AtomicBool::new(false)),
            create_calls: Arc::new(AtomicUsize::new(0)),
            keys: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight: Arc::new(AtomicUsize::new(0)),
            barrier: gather.map(|count| Arc::new(tokio::sync::Barrier::new(count))),
            barrier_enabled: Arc::new(AtomicBool::new(false)),
            slow_chunk: Arc::new(AtomicUsize::new(0)),
            slow_delay_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    fn fail_creates(&self) {
        self.fail_creates.store(true, Ordering::SeqCst);
    }

    fn enable_barrier(&self) {
        self.barrier_enabled.store(true, Ordering::SeqCst);
    }

    fn slow_creates(&self, chunk: usize, delay: std::time::Duration) {
        self.slow_chunk.store(chunk, Ordering::SeqCst);
        self.slow_delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    fn reset_observations(&self) {
        self.create_calls.store(0, Ordering::SeqCst);
        self.keys.lock().unwrap().clear();
        self.max_inflight.store(0, Ordering::SeqCst);
    }

    fn create_calls(&self) -> usize {
        self.create_calls.load(Ordering::SeqCst)
    }

    fn max_inflight(&self) -> usize {
        self.max_inflight.load(Ordering::SeqCst)
    }

    fn keys(&self) -> Vec<String> {
        let mut keys = self.keys.lock().unwrap().clone();
        keys.sort();
        keys
    }
}

#[async_trait]
impl CloudHeadStorage for InstrumentedHome {
    async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
        self.inner.read_head(key).await
    }

    async fn create_head(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        self.inner.create_head(key, bytes).await
    }

    async fn replace_head(
        &self,
        key: &str,
        expected: &CloudHeadVersion,
        bytes: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
        self.inner.replace_head(key, expected, bytes).await
    }

    async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
        self.inner.delete_probe_head(key).await
    }
}

#[async_trait]
impl CloudHome for InstrumentedHome {
    fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
        Some(self)
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.inner.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.inner.exists(key).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        match desired {
            CloudAccessState::Present { .. } => {
                Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: "test".to_string(),
                    region: "test".to_string(),
                    endpoint: None,
                    access_key: "test".to_string(),
                    secret_key: "test".to_string(),
                    key_prefix: None,
                }))
            }
            CloudAccessState::Absent { .. } => {
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl ExactSlotStorage for InstrumentedHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, CloudHomeError> {
        ExactSlotStorage::provider_binding(&self.inner).await
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ExactSlotStorage::allocate_slot(&self.inner, logical_key).await
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_creates.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "induced exact create failure".to_string(),
            ));
        }
        let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_inflight.fetch_max(current, Ordering::SeqCst);
        if self.barrier_enabled.load(Ordering::SeqCst) {
            self.barrier
                .as_ref()
                .expect("enabled barrier exists")
                .wait()
                .await;
        }
        let chunk = self.slow_chunk.load(Ordering::SeqCst);
        let result = if chunk == 0 {
            ExactSlotStorage::create_at(&self.inner, slot, body, progress).await
        } else {
            let bytes = body.collect().await?;
            let mut sent = 0;
            while sent < bytes.len() {
                sent = (sent + chunk).min(bytes.len());
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.slow_delay_ms.load(Ordering::SeqCst),
                ))
                .await;
                progress(sent as u64);
            }
            ExactSlotStorage::create_at(
                &self.inner,
                slot,
                ExactBlobBody::from_bytes(bytes),
                &crate::storage::cloud::no_progress(),
            )
            .await
        };
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        if result.is_ok() {
            self.keys
                .lock()
                .unwrap()
                .push(slot.logical_key().to_string());
        }
        result
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        ExactSlotStorage::read_at(&self.inner, slot).await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        ExactSlotStorage::read_range_at(&self.inner, slot, start, end).await
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), CloudFileReadError> {
        ExactSlotStorage::read_at_to_file(&self.inner, slot, destination).await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        ExactSlotStorage::delete_at(&self.inner, slot).await
    }
}

struct UploadFixture {
    db: Database,
    storage: CloudSyncStorage,
    home: Arc<InstrumentedHome>,
}

async fn upload_fixture(policy: crate::WritePolicy, uploads: usize) -> UploadFixture {
    upload_fixture_with_home(policy, uploads, Arc::new(InstrumentedHome::new())).await
}

async fn upload_fixture_with_home(
    policy: crate::WritePolicy,
    uploads: usize,
    home: Arc<InstrumentedHome>,
) -> UploadFixture {
    let limits = crate::blob::TransferLimits {
        uploads: std::num::NonZeroUsize::new(uploads).expect("nonzero upload limit"),
        downloads: std::num::NonZeroUsize::MIN,
    };
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        )),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        limits,
        policy,
        "test-device".to_string(),
        &test_migrations(),
    )
    .expect("open upload database");
    let owner = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        home.clone(),
        CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        BlobPathScheme::Hashed,
        "upload-store",
        owner.clone(),
    )
    .expect("construct exact sync storage")
    .with_test_serial_coordination(home.clone());
    create_exact_test_store(&db, &storage, "upload-store", &owner)
        .await
        .expect("initialize exact local blob authority");
    home.reset_observations();
    UploadFixture { db, storage, home }
}

async fn plant_uploads(
    fixture: &UploadFixture,
    store_dir: &StoreDir,
    rows: &[(&str, &[u8])],
    retain_pinned: bool,
) -> Vec<std::path::PathBuf> {
    let rows_owned = rows
        .iter()
        .map(|(id, bytes)| {
            (
                id.to_string(),
                bytes.len() as i64,
                crate::blob::content_hash(bytes),
            )
        })
        .collect::<Vec<_>>();
    fixture
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES (?1, 'upload', 0, '0000000001000-0000-test', '2024-01-01')",
                [ROOT_ID],
            )
            .map_err(DbError::from)?;
            for (id, size, hash) in rows_owned {
                conn.execute(
                    "INSERT INTO note_photos
                     (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES (?1, ?2, 'attach', ?3, ?4, '0000000001000-0000-test', '2024-01-01')",
                    rusqlite::params![id, ROOT_ID, size, hash],
                )
                .map_err(DbError::from)?;
            }
            Ok(())
        })
        .await
        .expect("plant exact Local blob rows");

    let mut paths = Vec::new();
    for (id, bytes) in rows {
        let path = store_dir
            .db_path()
            .parent()
            .expect("Store directory has a parent")
            .join(format!("{id}.source"));
        crate::local_blob::write_atomic(&path, bytes)
            .await
            .expect("write exact upload source");
        let row = fixture
            .db
            .row_blob_ref("note_photos", id)
            .await
            .expect("load exact Local row");
        let registered = path.clone();
        fixture
            .db
            .call(move |conn| Database::register_external_blob_on(conn, &row, &registered))
            .await
            .expect("register exact source authority");
        paths.push(path);
    }
    crate::blob::transition::make_remote(
        &fixture.db,
        store_dir,
        &Hlc::new("test-device".to_string()),
        "notes",
        ROOT_ID,
        retain_pinned,
    )
    .await
    .expect("enqueue real make_remote upload journals");
    paths
}

async fn run_drain(
    fixture: &UploadFixture,
    store_dir: &StoreDir,
    clock: &dyn Clock,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<DrainOutcome, DbError> {
    let routing = (fixture.db.write_policy() == crate::WritePolicy::Serial)
        .then(|| EncryptionService::from_key([42; 32]));
    drain_uploads(
        &fixture.db,
        &fixture.storage,
        store_dir,
        clock,
        &Hlc::new("test-device".to_string()),
        routing.as_ref(),
        observer,
    )
    .await
}

async fn journal(fixture: &UploadFixture, blob_id: &str) -> crate::db::OutboxEntry {
    fixture
        .db
        .get_pending_cloud_uploads()
        .await
        .expect("read upload journals")
        .into_iter()
        .find(|entry| {
            matches!(
                &entry.operation,
                crate::db::OutboxOperation::Upload { row, .. } if row.blob().id == blob_id
            )
        })
        .expect("upload journal exists")
}

async fn journal_attempt(
    fixture: &UploadFixture,
    blob_id: &str,
) -> (i64, Option<String>, Option<String>) {
    let blob_id = blob_id.to_string();
    fixture
        .db
        .call(move |conn| {
            conn.query_row(
                "SELECT attempt_count, last_error, last_attempt_at
                 FROM cloud_outbox WHERE operation = 'upload' AND row_id = ?1",
                [blob_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
        .expect("read journal attempt")
        .expect("journal exists")
}

fn is_created(entry: &crate::db::OutboxEntry) -> bool {
    matches!(
        entry.operation,
        crate::db::OutboxOperation::Upload {
            state: crate::db::OutboxUploadState::Created { .. },
            ..
        }
    )
}

fn created_slot(entry: &crate::db::OutboxEntry) -> &ObjectSlot {
    match &entry.operation {
        crate::db::OutboxOperation::Upload {
            state: crate::db::OutboxUploadState::Created { stored, .. },
            ..
        } => stored.object().slot(),
        _ => panic!("journal is not Created"),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ObsEvent {
    Started(String),
    Progress(String, u64, u64),
    Uploaded(String),
    Failed(String, String),
}

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

#[async_trait]
impl BlobTransitionObserver for RecordingObserver {
    async fn on_blob_upload_started(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Started(file_id.to_string()));
    }

    async fn on_blob_upload_progress(&self, file_id: &str, done: u64, total: u64) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Progress(file_id.to_string(), done, total));
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

#[async_trait]
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

#[tokio::test]
async fn provider_upload_failure_remains_typed_for_both_write_policies() {
    for policy in [
        crate::WritePolicy::MergeConcurrent,
        crate::WritePolicy::Serial,
    ] {
        let fixture = upload_fixture(policy, 1).await;
        let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        plant_uploads(
            &fixture,
            &store_dir,
            &[("fail0001", b"provider upload")],
            false,
        )
        .await;
        fixture.home.fail_creates();

        let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
            .await
            .unwrap();
        assert_eq!(outcome.failures.failures().len(), 1);
        assert!(outcome.failures.has_transport_failure());
        assert!(crate::sync::cycle::SyncCycleFailure::operation(
            "upload queued blob",
            outcome.failures,
        )
        .is_offline());
    }
}

#[tokio::test]
async fn bad_item_does_not_block_good_later_item() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let paths = plant_uploads(
        &fixture,
        &store_dir,
        &[("bad00001", b"bad"), ("good0001", b"good")],
        false,
    )
    .await;
    tokio::fs::remove_file(&paths[0]).await.unwrap();

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 1);
    assert_eq!(outcome.failures.failures().len(), 1);
    assert!(matches!(
        outcome.failures.failures()[0].cause,
        super::upload::UploadFailureCause::Storage(_)
    ));
    assert_eq!(journal_attempt(&fixture, "bad00001").await.0, 1);
    assert!(is_created(&journal(&fixture, "good0001").await));
}

#[tokio::test]
async fn upload_refuses_to_seal_while_a_rotation_is_pending() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("rotate01", b"bytes")], false).await;
    fixture.storage.shared_pending_rotation().mark_committed(2);

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 0);
    assert_eq!(fixture.home.create_calls(), 0);
    let (_, error, _) = journal_attempt(&fixture, "rotate01").await;
    assert!(error.unwrap().contains("rotated to generation 2"));
}

#[tokio::test]
async fn failure_persists_attempt_count_and_last_error() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("retry001", b"bytes")], false).await;
    fixture.home.fail_creates();

    run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    let (attempt, error, _) = journal_attempt(&fixture, "retry001").await;
    assert_eq!(attempt, 1);
    assert!(error.unwrap().contains("induced exact create failure"));

    run_drain(
        &fixture,
        &store_dir,
        &fixed_clock("2024-06-01T00:00:31Z"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(journal_attempt(&fixture, "retry001").await.0, 2);
    assert_eq!(fixture.home.create_calls(), 2);
}

#[tokio::test]
async fn backoff_skips_item_inside_window() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("backoff1", b"bytes")], false).await;
    fixture.home.fail_creates();
    let entry = journal(&fixture, "backoff1").await;
    fixture
        .db
        .record_cloud_outbox_failure(&entry, "prior", T0)
        .await
        .unwrap();

    let observer = RecordingObserver::new();
    run_drain(
        &fixture,
        &store_dir,
        &fixed_clock("2024-06-01T00:00:10Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(fixture.home.create_calls(), 0);
    assert!(observer.events().is_empty());
    assert_eq!(journal_attempt(&fixture, "backoff1").await.0, 1);

    run_drain(
        &fixture,
        &store_dir,
        &fixed_clock("2024-06-01T00:00:31Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(fixture.home.create_calls(), 1);
    assert_eq!(journal_attempt(&fixture, "backoff1").await.0, 2);
}

#[tokio::test]
async fn observer_fires_started_then_uploaded_on_success() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("observe1", b"bytes")], false).await;
    let observer = RecordingObserver::new();

    run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();

    let events = observer.events();
    assert_eq!(events.first(), Some(&ObsEvent::Started("observe1".into())));
    assert_eq!(events.last(), Some(&ObsEvent::Uploaded("observe1".into())));
    assert!(events.iter().any(|event| matches!(
        event,
        ObsEvent::Progress(id, done, total) if id == "observe1" && done == total
    )));
}

#[tokio::test]
async fn observer_fires_started_then_failed_on_failure() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("observe2", b"bytes")], false).await;
    fixture.home.fail_creates();
    let observer = RecordingObserver::new();

    run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();

    let events = observer.events();
    assert_eq!(events[0], ObsEvent::Started("observe2".into()));
    assert!(matches!(&events[1], ObsEvent::Failed(id, _) if id == "observe2"));
}

#[tokio::test(start_paused = true)]
async fn observer_receives_advancing_midfile_progress() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let bytes = vec![7; 10_000];
    plant_uploads(&fixture, &store_dir, &[("progress", &bytes)], false).await;
    fixture
        .home
        .slow_creates(1000, std::time::Duration::from_millis(500));
    let observer = RecordingObserver::new();

    run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();

    let progress = observer
        .events()
        .into_iter()
        .filter_map(|event| match event {
            ObsEvent::Progress(id, done, total) if id == "progress" => Some((done, total)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(progress.len() >= 2);
    assert!(progress.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    assert_eq!(progress.last().unwrap().0, progress.last().unwrap().1);
}

#[test]
fn backoff_window_is_exponential_and_capped() {
    assert_eq!(backoff_window(0), Duration::zero());
    assert_eq!(backoff_window(1), Duration::seconds(30));
    assert_eq!(backoff_window(2), Duration::seconds(60));
    assert_eq!(backoff_window(3), Duration::seconds(120));
    assert_eq!(backoff_window(8), Duration::seconds(3600));
    assert_eq!(backoff_window(50), Duration::seconds(3600));
}

#[tokio::test]
async fn enqueue_upload_on_is_transactional_with_host_writes() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let paths = plant_local_rows(&fixture, &store_dir, &[("transact", b"bytes")]).await;
    let row = fixture
        .db
        .row_blob_ref("note_photos", "transact")
        .await
        .unwrap();

    let rollback_row = row.clone();
    let rollback_path = paths[0].clone();
    fixture
        .db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            Database::enqueue_upload_on(
                &tx,
                "notes",
                ROOT_ID,
                &rollback_row,
                &rollback_path,
                false,
                T0,
            )?;
            tx.rollback().map_err(DbError::from)
        })
        .await
        .unwrap();
    assert!(fixture
        .db
        .get_pending_cloud_uploads()
        .await
        .unwrap()
        .is_empty());

    fixture
        .db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            Database::enqueue_upload_on(&tx, "notes", ROOT_ID, &row, &paths[0], false, T0)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(
        fixture.db.get_pending_cloud_uploads().await.unwrap().len(),
        1
    );
}

async fn plant_local_rows(
    fixture: &UploadFixture,
    store_dir: &StoreDir,
    rows: &[(&str, &[u8])],
) -> Vec<std::path::PathBuf> {
    let rows_owned = rows
        .iter()
        .map(|(id, bytes)| {
            (
                id.to_string(),
                bytes.len() as i64,
                crate::blob::content_hash(bytes),
            )
        })
        .collect::<Vec<_>>();
    fixture
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                 VALUES (?1, 'upload', 0, '0000000001000-0000-test', '2024-01-01')",
                [ROOT_ID],
            )
            .map_err(DbError::from)?;
            for (id, size, hash) in rows_owned {
                conn.execute(
                    "INSERT INTO note_photos
                     (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES (?1, ?2, 'attach', ?3, ?4, '0000000001000-0000-test', '2024-01-01')",
                    rusqlite::params![id, ROOT_ID, size, hash],
                )
                .map_err(DbError::from)?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let mut paths = Vec::new();
    for (id, bytes) in rows {
        let path = store_dir
            .db_path()
            .parent()
            .expect("Store directory has a parent")
            .join(format!("{id}.source"));
        crate::local_blob::write_atomic(&path, bytes).await.unwrap();
        let row = fixture.db.row_blob_ref("note_photos", id).await.unwrap();
        let registered = path.clone();
        fixture
            .db
            .call(move |conn| Database::register_external_blob_on(conn, &row, &registered))
            .await
            .unwrap();
        paths.push(path);
    }
    paths
}

#[tokio::test]
async fn pinned_upload_populates_the_protected_cache_folder() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let bytes = b"PINNED-AUDIO-BYTES";
    plant_uploads(&fixture, &store_dir, &[("pinaaaa1", bytes)], true).await;

    assert_eq!(
        run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
            .await
            .unwrap()
            .uploaded,
        1
    );
    let pinned = store_dir.pinned_blob_path("photos", "pinaaaa1").unwrap();
    assert_eq!(tokio::fs::read(pinned).await.unwrap(), bytes);
    assert!(!store_dir
        .cache_blob_path("photos", "pinaaaa1")
        .unwrap()
        .exists());
    let entry = journal(&fixture, "pinaaaa1").await;
    assert!(is_created(&entry));
    assert!(
        ExactSlotStorage::read_at(fixture.home.as_ref(), created_slot(&entry))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn unpinned_upload_populates_nothing_on_write() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    plant_uploads(&fixture, &store_dir, &[("unpaaaa1", b"UNPINNED")], false).await;

    run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert!(!store_dir
        .pinned_blob_path("photos", "unpaaaa1")
        .unwrap()
        .exists());
    assert!(!store_dir
        .cache_blob_path("photos", "unpaaaa1")
        .unwrap()
        .exists());
    assert!(is_created(&journal(&fixture, "unpaaaa1").await));
}

#[tokio::test]
async fn a_failed_pin_populate_does_not_fail_the_upload() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let pinned = store_dir.pinned_blob_path("photos", "pinfail1").unwrap();
    let shard = pinned.parent().unwrap().parent().unwrap();
    std::fs::create_dir_all(shard.parent().unwrap()).unwrap();
    std::fs::write(shard, b"blocker").unwrap();
    plant_uploads(&fixture, &store_dir, &[("pinfail1", b"PIN")], true).await;

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 0);
    assert_eq!(outcome.failures.failures().len(), 1);
    let entry = journal(&fixture, "pinfail1").await;
    assert!(is_created(&entry));
    assert!(
        ExactSlotStorage::read_at(fixture.home.as_ref(), created_slot(&entry))
            .await
            .is_ok()
    );
}

async fn seed_uploads(fixture: &UploadFixture, store_dir: &StoreDir, count: usize) -> Vec<String> {
    let owned = (0..count)
        .map(|index| {
            (
                format!("blob{index:04}"),
                format!("bytes-{index}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let borrowed = owned
        .iter()
        .map(|(id, bytes)| (id.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    plant_uploads(fixture, store_dir, &borrowed, false).await;
    owned.into_iter().map(|(id, _)| id).collect()
}

#[tokio::test]
async fn limit_one_drains_every_entry_in_order() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let ids = seed_uploads(&fixture, &store_dir, 3).await;
    let observer = RecordingObserver::new();

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 3);
    for id in &ids {
        assert!(is_created(&journal(&fixture, id).await));
    }
    let sequence = observer
        .events()
        .into_iter()
        .filter_map(|event| match event {
            ObsEvent::Started(id) => Some(format!("S{id}")),
            ObsEvent::Uploaded(id) => Some(format!("U{id}")),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sequence,
        vec![
            "Sblob0000",
            "Ublob0000",
            "Sblob0001",
            "Ublob0001",
            "Sblob0002",
            "Ublob0002"
        ]
    );
}

#[tokio::test]
async fn concurrent_drain_overlaps_up_to_the_limit() {
    let home = Arc::new(InstrumentedHome::with_barrier(Some(2)));
    let fixture =
        upload_fixture_with_home(crate::WritePolicy::MergeConcurrent, 2, home.clone()).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let ids = seed_uploads(&fixture, &store_dir, 4).await;
    home.enable_barrier();

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 4);
    assert_eq!(home.max_inflight(), 2);
    assert_eq!(home.keys().len(), 4);
    for id in ids {
        assert!(is_created(&journal(&fixture, &id).await));
    }
}

#[tokio::test]
async fn concurrent_drain_isolates_a_failed_upload() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 3).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let paths = plant_uploads(
        &fixture,
        &store_dir,
        &[
            ("good000a", b"aaa"),
            ("bad0000b", b"bbb"),
            ("good000c", b"ccc"),
        ],
        false,
    )
    .await;
    tokio::fs::remove_file(&paths[1]).await.unwrap();

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), None)
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 2);
    assert_eq!(outcome.failures.failures().len(), 1);
    assert!(is_created(&journal(&fixture, "good000a").await));
    assert!(is_created(&journal(&fixture, "good000c").await));
    assert_eq!(journal_attempt(&fixture, "bad0000b").await.0, 1);
}

#[tokio::test]
async fn paused_queue_admits_nothing_under_concurrency() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 3).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let ids = seed_uploads(&fixture, &store_dir, 3).await;
    let observer = PausingObserver::new(0);

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 0);
    assert_eq!(fixture.home.create_calls(), 0);
    assert!(observer.started().is_empty());
    for id in ids {
        assert!(!is_created(&journal(&fixture, &id).await));
    }
}

#[tokio::test]
async fn pause_after_first_finishes_inflight_and_stops_admitting() {
    let fixture = upload_fixture(crate::WritePolicy::MergeConcurrent, 1).await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let ids = seed_uploads(&fixture, &store_dir, 2).await;
    let observer = PausingObserver::new(1);

    let outcome = run_drain(&fixture, &store_dir, &fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert_eq!(outcome.uploaded, 1);
    assert_eq!(observer.started(), vec![ids[0].clone()]);
    assert!(is_created(&journal(&fixture, &ids[0]).await));
    assert!(!is_created(&journal(&fixture, &ids[1]).await));
}
