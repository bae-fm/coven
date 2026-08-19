//! Tests for the exact blob upload journal and drain.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::blob::DrainOutcome;
use crate::sync::test_helpers::{test_migrations, test_synced_tables_with_blob};
use coven_database::Database;
use coven_database::StoreDatabase;
use coven_foundation::clock::{Clock, FixedClock};
use coven_foundation::store_dir::StoreDir;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::{
    BlobRef, BlobScope, BlobTransitionObserver, CacheFill, Provenance, RowBlobAuthority, RowBlobRef,
};
use coven_protocol::objects::ObjectSlot;
use coven_protocol::synced_schema::BlobDecl;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::cloud::{
    BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudFileReadError, CloudHome,
    CloudHomeError, CloudHomeJoinInfo, ExactCreateOutcome, ExactSlotStorage, ExactUpload,
    RevokeOutcome, UploadProgress,
};
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

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

    fn exact_reads(&self) -> Vec<ObjectSlot> {
        self.inner.exact_reads()
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
impl CloudHome for InstrumentedHome {
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
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        ExactSlotStorage::provider_binding(&self.inner).await
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ExactSlotStorage::allocate_slot(&self.inner, logical_key).await
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        progress: &UploadProgress,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
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
            ExactSlotStorage::create_at(&self.inner, upload, progress).await
        } else {
            let bytes = upload.body().await?.collect().await?;
            let mut sent = 0;
            while sent < bytes.len() {
                sent = (sent + chunk).min(bytes.len());
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.slow_delay_ms.load(Ordering::SeqCst),
                ))
                .await;
                progress(sent as u64);
            }
            ExactSlotStorage::create_at(&self.inner, upload, &coven_storage::cloud::no_progress())
                .await
        };
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        if result.is_ok() {
            self.keys
                .lock()
                .unwrap()
                .push(upload.object().slot().logical_key().to_string());
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
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<(), CloudFileReadError> {
        ExactSlotStorage::read_at_to_file(&self.inner, slot, destination, progress).await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        ExactSlotStorage::delete_at(&self.inner, slot).await
    }
}

struct UploadFixture {
    db: Database,
    database: StoreDatabase,
    store_dir: StoreDir,
    device: crate::sync::test_helpers::TestDevice,
    storage: Arc<CloudSyncConnection>,
    home: Arc<InstrumentedHome>,
}

impl UploadFixture {
    async fn new(uploads: usize) -> Self {
        Self::with_home(uploads, Arc::new(InstrumentedHome::new())).await
    }

    async fn with_home(uploads: usize, home: Arc<InstrumentedHome>) -> Self {
        let limits = coven_protocol::blob::TransferLimits {
            uploads: std::num::NonZeroUsize::new(uploads).expect("nonzero upload limit"),
            downloads: std::num::NonZeroUsize::MIN,
        };
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = Database::open_synthetic_for_test(
            std::path::Path::new(":memory:"),
            db_store_dir.clone(),
            test_synced_tables_with_blob(BlobDecl::new(
                "photos",
                Provenance::UserProvided,
                CacheFill::CacheLazy,
            )),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            limits,
            "test-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open upload database");
        let owner = UserKeypair::generate();
        let storage = Arc::new(CloudSyncConnection::new(
            home.clone(),
            CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            BlobPathScheme::Hashed,
            "upload-store",
            owner.clone(),
        ));
        let device = crate::sync::test_helpers::TestDevice::create(
            &db,
            db_store_dir.clone(),
            storage.clone(),
            "upload-store",
            owner,
        )
        .await
        .expect("initialize exact local blob authority");
        home.reset_observations();
        let database = coven_database::StoreDatabase::new(&db);
        Self {
            db,
            database,
            store_dir: db_store_dir,
            device,
            storage,
            home,
        }
    }

    async fn drain(
        &self,
        clock: &dyn Clock,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<DrainOutcome, crate::sync::test_helpers::TestError> {
        self.device.drain_uploads(clock, None, observer).await
    }

    async fn journal(&self, blob_id: &str) -> coven_database::OutboxEntry {
        coven_database::StoreDatabase::new(&self.db)
            .pending_blob_uploads()
            .await
            .expect("read upload journals")
            .into_iter()
            .find(|entry| {
                matches!(
                    &entry.operation,
                    coven_database::OutboxOperation::Upload { row, .. }
                        if row.blob().id == blob_id
                )
            })
            .expect("upload journal exists")
    }

    async fn journal_attempt(&self, blob_id: &str) -> coven_database::OutboxAttempt {
        let blob_id = blob_id.to_string();
        self.db
            .upload_outbox_attempt_for_test(&blob_id)
            .await
            .expect("read journal attempt")
            .expect("journal exists")
    }

    async fn plant_local_rows(&self, rows: &[(&str, &[u8])]) -> Vec<std::path::PathBuf> {
        self.db
            .insert_local_upload_rows_for_test(ROOT_ID, rows)
            .await
            .expect("plant exact Local blob rows");
        let mut paths = Vec::new();
        for (id, bytes) in rows {
            let path = self
                .store_dir
                .db_path()
                .parent()
                .expect("Store directory has a parent")
                .join(format!("{id}.source"));
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&path, bytes)
                .await
                .expect("write exact upload source");
            coven_database::StoreDatabase::new(&self.db)
                .register_external_blob_for_test("note_photos", id, &path)
                .await;
            paths.push(path);
        }
        paths
    }

    async fn plant_uploads(
        &self,
        rows: &[(&str, &[u8])],
        retain_pinned: bool,
    ) -> Vec<std::path::PathBuf> {
        let paths = self.plant_local_rows(rows).await;
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            self.database.clone(),
            self.store_dir.clone(),
        )
        .make_remote("notes", ROOT_ID, retain_pinned)
        .await
        .expect("enqueue real make_remote upload journals");
        paths
    }

    async fn seed_uploads(&self, count: usize) -> Vec<String> {
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
        self.plant_uploads(&borrowed, false).await;
        owned.into_iter().map(|(id, _)| id).collect()
    }
}

fn is_created(entry: &coven_database::OutboxEntry) -> bool {
    matches!(
        entry.operation,
        coven_database::OutboxOperation::Upload {
            state: coven_database::OutboxUploadState::Created { .. },
            ..
        }
    )
}

fn created_slot(entry: &coven_database::OutboxEntry) -> &ObjectSlot {
    match &entry.operation {
        coven_database::OutboxOperation::Upload {
            state: coven_database::OutboxUploadState::Created { stored, .. },
            ..
        } => stored.object().slot(),
        _ => panic!("journal is not Created"),
    }
}

fn created_stored(
    entry: &coven_database::OutboxEntry,
) -> &coven_protocol::blob::locator::StoredBlobRef {
    match &entry.operation {
        coven_database::OutboxOperation::Upload {
            state: coven_database::OutboxUploadState::Created { stored, .. },
            ..
        } => stored,
        _ => panic!("journal is not Created"),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ObsEvent {
    Preparing(String),
    PreparationProgress(String, u64, u64),
    Started(String),
    Progress(String, u64, u64),
    Uploaded(String),
    Failed(String, String),
}

struct RecordingObserver {
    events: Mutex<Vec<ObsEvent>>,
}

#[derive(Default)]
struct IdentityObserver {
    started: Mutex<Vec<RowBlobRef>>,
}

#[async_trait]
impl BlobTransitionObserver for IdentityObserver {
    async fn on_blob_upload_started(&self, upload: &RowBlobRef) {
        self.started.lock().unwrap().push(upload.clone());
    }

    async fn on_blob_uploaded(&self, _upload: &RowBlobRef) {}
    async fn on_blob_upload_failed(&self, _upload: &RowBlobRef, _error: &str) {}
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
    async fn on_blob_preparation_started(&self, upload: &RowBlobRef) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Preparing(upload.blob().id.clone()));
    }

    async fn on_blob_preparation_progress(&self, upload: &RowBlobRef, done: u64, total: u64) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::PreparationProgress(
                upload.blob().id.clone(),
                done,
                total,
            ));
    }

    async fn on_blob_upload_started(&self, upload: &RowBlobRef) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Started(upload.blob().id.clone()));
    }

    async fn on_blob_upload_progress(&self, upload: &RowBlobRef, done: u64, total: u64) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Progress(upload.blob().id.clone(), done, total));
    }

    async fn on_blob_uploaded(&self, upload: &RowBlobRef) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Uploaded(upload.blob().id.clone()));
    }

    async fn on_blob_upload_failed(&self, upload: &RowBlobRef, error: &str) {
        self.events.lock().unwrap().push(ObsEvent::Failed(
            upload.blob().id.clone(),
            error.to_string(),
        ));
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
    async fn on_blob_upload_started(&self, upload: &RowBlobRef) {
        self.started.lock().unwrap().push(upload.blob().id.clone());
    }

    async fn on_blob_uploaded(&self, _upload: &RowBlobRef) {}
    async fn on_blob_upload_failed(&self, _upload: &RowBlobRef, _error: &str) {}

    fn should_skip_uploads(&self) -> bool {
        self.checks.fetch_add(1, Ordering::SeqCst) >= self.admit_before
    }
}

/// An empty queue is its own answer, not a zero count. This is the disposition
/// that separates "there was nothing to do" from "the work was done" — the two a
/// caller cannot tell apart from `uploaded: 0` alone, and the pair that made an
/// explicit drain racing a sync cycle's drain look like a success to the loser.
#[tokio::test]
async fn empty_queue_reports_itself_rather_than_a_zero_count() {
    let fixture = UploadFixture::new(1).await;

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert!(matches!(outcome, DrainOutcome::QueueEmpty));
    assert_eq!(fixture.home.create_calls(), 0);
}

#[tokio::test]
async fn successful_drain_does_not_read_uploaded_body_from_provider() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("noread01", b"retained spool bytes")], false)
        .await;
    let reads_before = fixture.home.exact_reads().len();

    let outcome = fixture
        .drain(&fixed_clock(T0), None)
        .await
        .expect("publish queued blob");

    assert_eq!(outcome.uploaded(), 1);
    let entry = fixture.journal("noread01").await;
    assert!(
        !fixture.home.exact_reads()[reads_before..].contains(created_slot(&entry)),
        "the successful create response verifies the exact blob without fetching its body",
    );
}

#[tokio::test]
async fn upload_observer_receives_the_exact_blob_bearing_row() {
    let fixture = UploadFixture::new(1).await;
    let bytes = b"cover bytes";
    let source_path = fixture
        .store_dir
        .db_path()
        .parent()
        .expect("Store directory has a parent")
        .join("cover.source");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&source_path, bytes)
        .await
        .expect("write cover source");
    let row = RowBlobRef::new(
        "note_covers".to_string(),
        "release-row".to_string(),
        "cover-stamp".to_string(),
        "cover".to_string(),
        BlobRef {
            namespace: "covers".to_string(),
            id: "cover-blob".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::UserProvided,
            fill: CacheFill::CacheLazy,
        },
        bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(bytes),
        RowBlobAuthority::Local,
        None,
    )
    .expect("valid cover row blob");
    fixture
        .db
        .enqueue_blob_upload_with_retention_for_test(
            "notes",
            ROOT_ID,
            row.clone(),
            source_path,
            false,
            T0,
        )
        .await
        .expect("enqueue cover upload");
    fixture.home.fail_creates();
    let observer = IdentityObserver::default();

    let _ = fixture.drain(&fixed_clock(T0), Some(&observer)).await;

    assert_eq!(*observer.started.lock().unwrap(), vec![row]);
}

/// A pass that finishes what an earlier pass created counts no upload — the
/// object was already written — but it is a `Drained` pass, not an empty one:
/// the entry was attempted and retired here.
#[tokio::test]
async fn a_second_pass_over_a_created_entry_drains_without_counting_it() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("twice001", b"bytes")], false)
        .await;

    assert_eq!(
        fixture
            .drain(&fixed_clock(T0), None)
            .await
            .unwrap()
            .uploaded(),
        1
    );
    assert!(is_created(&fixture.journal("twice001").await));

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(
        outcome.uploaded(),
        0,
        "the object was created by the first pass, so this one creates none",
    );
    assert!(outcome.failures().failures().is_empty());
    assert_eq!(
        fixture.home.create_calls(),
        1,
        "and no second object was written for it",
    );
}

#[tokio::test]
async fn provider_upload_failure_remains_typed() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("fail0001", b"provider upload")], false)
        .await;
    fixture.home.fail_creates();

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.failures().failures().len(), 1);
    assert!(outcome.failures().has_transport_failure());
    assert!(crate::sync::cycle::SyncCycleFailure::operation(
        "upload queued blob",
        outcome.into_failures(),
    )
    .is_offline());
}

#[tokio::test]
async fn bad_item_does_not_block_good_later_item() {
    let fixture = UploadFixture::new(1).await;
    let paths = fixture
        .plant_uploads(&[("bad00001", b"bad"), ("good0001", b"good")], false)
        .await;
    tokio::fs::remove_file(&paths[0]).await.unwrap();

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.uploaded(), 1);
    assert_eq!(outcome.failures().failures().len(), 1);
    assert!(matches!(
        outcome.failures().failures()[0].cause,
        crate::blob::UploadFailureCause::Storage(_)
    ));
    assert_eq!(fixture.journal_attempt("bad00001").await.0, 1);
    assert!(is_created(&fixture.journal("good0001").await));
}

#[tokio::test]
async fn upload_refuses_to_seal_while_a_rotation_is_pending() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("rotate01", b"bytes")], false)
        .await;
    fixture.storage.mark_rotation_committed_for_test(2).unwrap();

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.uploaded(), 0);
    assert_eq!(fixture.home.create_calls(), 0);
    let (_, error, _) = fixture.journal_attempt("rotate01").await;
    assert!(error.unwrap().contains("PeerCommitted { generation: 2 }"));
}

#[tokio::test]
async fn failure_persists_attempt_count_and_last_error() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("retry001", b"bytes")], false)
        .await;
    fixture.home.fail_creates();

    fixture.drain(&fixed_clock(T0), None).await.unwrap();
    let (attempt, error, _) = fixture.journal_attempt("retry001").await;
    assert_eq!(attempt, 1);
    assert!(error.unwrap().contains("induced exact create failure"));

    fixture
        .drain(&fixed_clock("2024-06-01T00:00:31Z"), None)
        .await
        .unwrap();
    assert_eq!(fixture.journal_attempt("retry001").await.0, 2);
    assert_eq!(fixture.home.create_calls(), 2);
}

#[tokio::test]
async fn backoff_skips_item_inside_window() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("backoff1", b"bytes")], false)
        .await;
    fixture.home.fail_creates();
    let entry = fixture.journal("backoff1").await;
    coven_database::StoreDatabase::new(&fixture.db)
        .record_outbox_failure(&entry, "prior", T0)
        .await
        .unwrap();

    let observer = RecordingObserver::new();
    let outcome = fixture
        .drain(&fixed_clock("2024-06-01T00:00:10Z"), Some(&observer))
        .await
        .unwrap();
    // The entry is still queued, just not due — reported as its own disposition
    // so a caller cannot read the skipped pass as a drained queue.
    assert!(matches!(outcome, DrainOutcome::AllInBackoff));
    assert_eq!(fixture.home.create_calls(), 0);
    assert!(observer.events().is_empty());
    assert_eq!(fixture.journal_attempt("backoff1").await.0, 1);

    fixture
        .drain(&fixed_clock("2024-06-01T00:00:31Z"), Some(&observer))
        .await
        .unwrap();
    assert_eq!(fixture.home.create_calls(), 1);
    assert_eq!(fixture.journal_attempt("backoff1").await.0, 2);
}

#[tokio::test]
async fn corrupt_upload_backoff_timestamp_fails_before_remote_effects() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("healthy1", b"healthy"), ("badtime1", b"corrupt")], false)
        .await;
    let entry = fixture.journal("badtime1").await;
    let entry_id = entry.id;
    fixture
        .db
        .corrupt_upload_outbox_attempt_time_for_test(entry_id)
        .await
        .expect("corrupt last_attempt_at");

    let result = fixture
        .drain(&fixed_clock("2024-06-01T00:00:10Z"), None)
        .await;
    let error = match result {
        Ok(_) => panic!("a corrupt retry timestamp must fail the drain"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("unparseable last_attempt_at"),
        "{error}"
    );
    assert_eq!(
        fixture.home.create_calls(),
        0,
        "the corrupt entry produces no remote effect",
    );
    assert_eq!(
        fixture.journal_attempt("badtime1").await.0,
        1,
        "the corrupt journal row remains unchanged",
    );
    assert_eq!(
        fixture.journal_attempt("healthy1").await.0,
        0,
        "the earlier healthy journal row remains unchanged",
    );
}

/// The queue has two drainers — the sync cycle's and the host's explicit one —
/// and before they took turns both could read the same pending entry and run a
/// full attempt on it. An entry is claimed only by the compare-and-set that
/// hands off its prepared object, so the loser had already sealed the whole
/// blob, written a spool, and reported preparation progress for it. A host
/// observer watching one blob's preparation saw two interleaved streams and a
/// byte count that went backwards.
///
/// Both assertions below are that regression, from the two directions it was
/// visible: no blob is prepared twice, and no blob's reported preparation ever
/// goes backwards.
#[tokio::test]
async fn concurrent_drains_never_prepare_one_entry_twice() {
    let fixture = UploadFixture::new(4).await;
    let ids = fixture.seed_uploads(6).await;
    let observer = RecordingObserver::new();

    // The drain ahead awaits the filesystem and the provider throughout, so the
    // one behind is polled well inside it — before taking turns, deep enough to
    // read the same queue and admit the same entries.
    let clock = fixed_clock(T0);
    let (first, second) = tokio::join!(
        fixture.drain(&clock, Some(&observer)),
        fixture.drain(&clock, Some(&observer)),
    );
    first.expect("first drain");
    second.expect("second drain");

    let events = observer.events();
    let mut prepared = events
        .iter()
        .filter_map(|event| match event {
            ObsEvent::Preparing(id) => Some(id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    prepared.sort();
    let mut once = prepared.clone();
    once.dedup();
    assert_eq!(
        prepared, once,
        "a blob was admitted by both drains and prepared twice: {prepared:?}",
    );
    assert_eq!(
        once.len(),
        ids.len(),
        "every queued blob is prepared exactly once across both drains",
    );

    for id in &ids {
        let mut reported = 0;
        for event in &events {
            let ObsEvent::PreparationProgress(event_id, done, total) = event else {
                continue;
            };
            if event_id != id {
                continue;
            }
            assert!(
                *done >= reported,
                "preparation progress for {id} regressed: previous {reported}, \
                 received {done} of {total}",
            );
            reported = *done;
        }
    }
}

#[tokio::test]
async fn observer_fires_started_then_uploaded_on_success() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("observe1", b"bytes")], false)
        .await;
    let observer = RecordingObserver::new();

    fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();

    let events = observer.events();
    assert_eq!(
        events.first(),
        Some(&ObsEvent::Preparing("observe1".into()))
    );
    let preparation_finished = events
        .iter()
        .position(|event| matches!(event, ObsEvent::PreparationProgress(id, done, total) if id == "observe1" && done == total))
        .expect("preparation reports completion");
    let upload_started = events
        .iter()
        .position(|event| event == &ObsEvent::Started("observe1".into()))
        .expect("upload reports its actual provider start");
    assert!(preparation_finished < upload_started);
    assert_eq!(events.last(), Some(&ObsEvent::Uploaded("observe1".into())));
    assert!(events.iter().any(|event| matches!(
        event,
        ObsEvent::Progress(id, done, total) if id == "observe1" && done == total
    )));
}

#[tokio::test]
async fn observer_fires_started_then_failed_on_failure() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("observe2", b"bytes")], false)
        .await;
    fixture.home.fail_creates();
    let observer = RecordingObserver::new();

    fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();

    let events = observer.events();
    assert_eq!(events[0], ObsEvent::Preparing("observe2".into()));
    let upload_started = events
        .iter()
        .position(|event| event == &ObsEvent::Started("observe2".into()))
        .expect("provider upload starts after preparation");
    assert!(matches!(&events[upload_started + 1], ObsEvent::Failed(id, _) if id == "observe2"));
}

#[tokio::test(start_paused = true)]
async fn observer_receives_advancing_midfile_progress() {
    let fixture = UploadFixture::new(1).await;
    let bytes = vec![7; 10_000];
    fixture.plant_uploads(&[("progress", &bytes)], false).await;
    fixture
        .home
        .slow_creates(1000, std::time::Duration::from_millis(500));
    let observer = RecordingObserver::new();

    fixture
        .drain(&fixed_clock(T0), Some(&observer))
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

#[tokio::test]
async fn enqueue_upload_on_is_transactional_with_host_writes() {
    let fixture = UploadFixture::new(1).await;
    let paths = fixture.plant_local_rows(&[("transact", b"bytes")]).await;
    let row = fixture
        .db
        .row_blob_ref("note_photos", "transact")
        .await
        .unwrap();

    let rollback_row = row.clone();
    let rollback_path = paths[0].clone();
    fixture
        .db
        .roll_back_blob_upload_for_test("notes", ROOT_ID, rollback_row, rollback_path, T0)
        .await
        .unwrap();
    assert!(coven_database::StoreDatabase::new(&fixture.db)
        .pending_blob_uploads()
        .await
        .unwrap()
        .is_empty());

    fixture
        .db
        .enqueue_blob_upload_with_retention_for_test(
            "notes",
            ROOT_ID,
            row,
            paths[0].clone(),
            false,
            T0,
        )
        .await
        .unwrap();
    assert_eq!(
        coven_database::StoreDatabase::new(&fixture.db)
            .pending_blob_uploads()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn pinned_upload_populates_the_protected_cache_folder() {
    let fixture = UploadFixture::new(1).await;
    let bytes = b"PINNED-AUDIO-BYTES";
    fixture.plant_uploads(&[("pinaaaa1", bytes)], true).await;

    assert_eq!(
        fixture
            .drain(&fixed_clock(T0), None)
            .await
            .unwrap()
            .uploaded(),
        1
    );
    let entry = fixture.journal("pinaaaa1").await;
    let locator_hash = created_stored(&entry).locator().locator_hash();
    let pinned = fixture
        .store_dir
        .pinned_blob_path("photos", locator_hash)
        .unwrap();
    assert_eq!(tokio::fs::read(pinned).await.unwrap(), bytes);
    assert!(!fixture
        .store_dir
        .cache_blob_path("photos", locator_hash)
        .unwrap()
        .exists());
    assert!(is_created(&entry));
    assert!(
        ExactSlotStorage::read_at(fixture.home.as_ref(), created_slot(&entry))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn unpinned_upload_populates_nothing_on_write() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("unpaaaa1", b"UNPINNED")], false)
        .await;

    fixture.drain(&fixed_clock(T0), None).await.unwrap();
    let entry = fixture.journal("unpaaaa1").await;
    let locator_hash = created_stored(&entry).locator().locator_hash();
    assert!(!fixture
        .store_dir
        .pinned_blob_path("photos", locator_hash)
        .unwrap()
        .exists());
    assert!(!fixture
        .store_dir
        .cache_blob_path("photos", locator_hash)
        .unwrap()
        .exists());
    assert!(is_created(&entry));
}

#[tokio::test]
async fn a_failed_pin_populate_does_not_fail_the_upload() {
    let fixture = UploadFixture::new(1).await;
    let pinned_namespace = fixture
        .store_dir
        .storage_dir()
        .join("pinned")
        .join("photos");
    std::fs::create_dir_all(pinned_namespace.parent().unwrap()).unwrap();
    std::fs::write(&pinned_namespace, b"blocker").unwrap();
    fixture.plant_uploads(&[("pinfail1", b"PIN")], true).await;

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.uploaded(), 0);
    assert_eq!(outcome.failures().failures().len(), 1);
    let entry = fixture.journal("pinfail1").await;
    assert!(is_created(&entry));
    assert!(
        ExactSlotStorage::read_at(fixture.home.as_ref(), created_slot(&entry))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn limit_one_drains_every_entry_in_order() {
    let fixture = UploadFixture::new(1).await;
    let ids = fixture.seed_uploads(3).await;
    let observer = RecordingObserver::new();

    let outcome = fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert_eq!(outcome.uploaded(), 3);
    for id in &ids {
        assert!(is_created(&fixture.journal(id).await));
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
    let fixture = UploadFixture::with_home(2, home.clone()).await;
    let ids = fixture.seed_uploads(4).await;
    home.enable_barrier();

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.uploaded(), 4);
    assert_eq!(home.max_inflight(), 2);
    assert_eq!(home.keys().len(), 4);
    for id in ids {
        assert!(is_created(&fixture.journal(&id).await));
    }
}

#[tokio::test]
async fn concurrent_drain_isolates_a_failed_upload() {
    let fixture = UploadFixture::new(3).await;
    let paths = fixture
        .plant_uploads(
            &[
                ("good000a", b"aaa"),
                ("bad0000b", b"bbb"),
                ("good000c", b"ccc"),
            ],
            false,
        )
        .await;
    tokio::fs::remove_file(&paths[1]).await.unwrap();

    let outcome = fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert_eq!(outcome.uploaded(), 2);
    assert_eq!(outcome.failures().failures().len(), 1);
    assert!(is_created(&fixture.journal("good000a").await));
    assert!(is_created(&fixture.journal("good000c").await));
    assert_eq!(fixture.journal_attempt("bad0000b").await.0, 1);
}

#[tokio::test]
async fn paused_queue_admits_nothing_under_concurrency() {
    let fixture = UploadFixture::new(3).await;
    let ids = fixture.seed_uploads(3).await;
    let observer = PausingObserver::new(0);

    let outcome = fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    // The pass reports the pause rather than a zero count, so a caller cannot
    // read "the host has uploads held" as "the queue is drained".
    assert!(matches!(outcome, DrainOutcome::Paused));
    assert_eq!(fixture.home.create_calls(), 0);
    assert!(observer.started().is_empty());
    for id in ids {
        assert!(!is_created(&fixture.journal(&id).await));
    }
}

#[tokio::test]
async fn pause_after_first_finishes_inflight_and_stops_admitting() {
    let fixture = UploadFixture::new(1).await;
    let ids = fixture.seed_uploads(2).await;
    let observer = PausingObserver::new(1);

    let outcome = fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert_eq!(outcome.uploaded(), 1);
    assert_eq!(observer.started(), vec![ids[0].clone()]);
    assert!(is_created(&fixture.journal(&ids[0]).await));
    assert!(!is_created(&fixture.journal(&ids[1]).await));
}
