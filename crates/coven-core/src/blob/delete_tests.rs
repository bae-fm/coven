//! Tests for the blob-delete half: signed tombstones, the graced GC that performs
//! the actual deletion, exact upload/delete intent independence, live-reference
//! tombstone cancellation, and the shared `cloud_outbox` row shape.
//!
//! The grace and forgery behaviors are the load-bearing ones — this code deletes
//! user data and trusts a signature, so a stale or forged tombstone is real data
//! loss.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::RwLock;

use async_trait::async_trait;

use crate::blob::delete::{
    drain_tombstones, gc_tombstones, BlobTombstoneJson, BLOB_TOMBSTONE_GRACE,
};
use crate::blob::{BlobScope, CacheFill, Provenance};
use crate::clock::FixedClock;
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::storage::cloud::{no_progress, CloudHome, CloudHomeError};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
use crate::sync::membership::MemberRole;
use crate::sync::session::BlobDecl;
use crate::sync::storage::SyncStorage;
use crate::sync::store::StoreDatabase;
use crate::sync::test_helpers::{
    exec, open_test_db, open_test_db_with_blob, plant_blob_row, pubkey_hex, TestStore,
};
use rusqlite::OptionalExtension;

const T0: &str = "2024-06-01T00:00:00Z";

/// A wall-clock instant for the tests, fixed so the grace math is deterministic.
fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid rfc3339")
        .with_timezone(&chrono::Utc)
}

/// A `Database` over an in-memory connection with just the bookkeeping tables.
/// The `cloud_outbox` table both operations share is created by `Database::open`.
fn open_outbox_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        &[],
    )
    .expect("open outbox database");
    db
}

/// Load the membership already activated in `db` and run the GC. Store fixtures
/// create or open that exact Store in the same database before calling this;
/// this helper never reconstructs a join.
async fn gc_tombstones_anchored(
    db: &Database,
    storage: &TestStore,
    cloud_home: &dyn CloudHome,
    cipher: &RwLock<CloudCipher>,
    store_id: &str,
    clock: &dyn crate::clock::Clock,
    grace: chrono::Duration,
) -> Result<usize, String> {
    let self_pubkey = storage.protocol_founder_pubkey();
    gc_tombstones_as(
        &self_pubkey,
        db,
        storage,
        cloud_home,
        cipher,
        store_id,
        clock,
        grace,
    )
    .await
}

/// Run `gc_tombstones` as `self_pubkey` against the membership already activated
/// in `db`, so a test can exercise owner and member reclaim authorization.
#[allow(clippy::too_many_arguments)]
async fn gc_tombstones_as(
    self_pubkey: &str,
    db: &Database,
    storage: &TestStore,
    cloud_home: &dyn CloudHome,
    cipher: &RwLock<CloudCipher>,
    store_id: &str,
    clock: &dyn crate::clock::Clock,
    grace: chrono::Duration,
) -> Result<usize, String> {
    let membership = crate::sync::store::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(db),
    )
    .await
    .map_err(|e| e.to_string())?;
    let activated_uploaders = StoreDatabase::new(db)
        .activated_store_device_registration_records()
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect();
    let membership = membership
        .chain
        .as_ref()
        .ok_or_else(|| "tombstone GC test Store has no membership chain".to_string())?;
    gc_tombstones(
        db,
        cloud_home,
        &storage.storage,
        cipher,
        store_id,
        self_pubkey,
        &activated_uploaders,
        membership,
        clock,
        grace,
    )
    .await
}

async fn gc_tombstones_without_live_refs(
    db: &Database,
    storage: &TestStore,
    cloud_home: &dyn CloudHome,
    cipher: &RwLock<CloudCipher>,
    store_id: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<usize, String> {
    gc_tombstones_anchored(
        db,
        storage,
        cloud_home,
        cipher,
        store_id,
        clock,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
}

/// Read back `(attempt_count, last_error, last_attempt_at)` for a delete entry, or
/// `None` if it was removed.
async fn get_delete(db: &Database, id: i64) -> Option<(i64, Option<String>, Option<String>)> {
    db.call(move |conn| {
        conn.query_row(
            "SELECT attempt_count, last_error, last_attempt_at FROM cloud_outbox \
             WHERE id = ?1 AND operation = 'delete'",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(DbError::from)
    })
    .await
    .expect("query delete outbox entry")
}

/// A plaintext cipher (tests don't encrypt) behind the lock the drain/GC take.
fn plaintext_cipher() -> RwLock<CloudCipher> {
    RwLock::new(CloudCipher::Plaintext)
}

fn create_exact_blob<'a>(
    storage: &'a TestStore,
    namespace: &'a str,
    id: &'a str,
    bytes: &'a [u8],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::blob::locator::StoredBlobRef> + 'a>>
{
    Box::pin(async move {
        let (uploader, registration, _) = storage
            .founder_device_authority()
            .await
            .expect("load exact founder device authority");
        let authority = crate::sync::storage::BlobWriteAuthority::new(&uploader, &registration)
            .expect("validate exact blob write authority");
        create_exact_blob_with_authority(&storage.storage, &authority, namespace, id, bytes).await
    })
}

fn create_exact_blob_as<'a>(
    storage: &'a TestStore,
    db: &'a Database,
    identity: &'a UserKeypair,
    namespace: &'a str,
    id: &'a str,
    bytes: &'a [u8],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::blob::locator::StoredBlobRef> + 'a>>
{
    Box::pin(async move {
        let (uploader, registration) = StoreDatabase::new(db)
            .local_blob_write_authority()
            .await
            .expect("load activated exact blob write authority");
        let device_signer = registration
            .device_signer(identity)
            .expect("derive exact device signer");
        let authority = crate::sync::storage::BlobWriteAuthority::new(&uploader, &registration)
            .expect("validate exact blob write authority");
        assert_eq!(
            authority.registration.device_signing_pubkey,
            hex::encode(device_signer.public_key())
        );
        let member_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
            storage.home.clone(),
            CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key([42; 32])),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            "test-store",
            identity.clone(),
        )
        .expect("construct exact member blob storage");
        create_exact_blob_with_authority(&member_storage, &authority, namespace, id, bytes).await
    })
}

async fn create_exact_blob_with_authority(
    storage: &dyn SyncStorage,
    authority: &crate::sync::storage::BlobWriteAuthority<'_>,
    namespace: &str,
    id: &str,
    bytes: &[u8],
) -> crate::blob::locator::StoredBlobRef {
    let protection = crate::encryption::EncryptionService::from_key([42; 32]);
    let locator = crate::blob::locator::BlobLocator::opaque(
        namespace,
        id,
        authority.reference.clone(),
        crate::blob::locator::RemoteAudience::Store,
        BlobScope::Master,
        protection.seal_key_fingerprint(),
        bytes.len() as u64,
        crate::sync::store_commit::ObjectHash::digest(bytes),
    )
    .expect("build exact blob locator");
    let temp = tempfile::tempdir().expect("create exact blob spool directory");
    let plaintext = temp.path().join("plaintext");
    let spool = temp.path().join("stored");
    crate::local_blob::write_atomic(&plaintext, bytes)
        .await
        .expect("write exact blob plaintext");
    let slot = storage
        .allocate_blob_slot(&locator, authority)
        .await
        .expect("allocate exact blob slot");
    storage
        .seal_blob_to_spool(
            &locator,
            authority,
            crate::sync::storage::BlobSpoolProtection::Opaque(protection),
            &plaintext,
            &spool,
        )
        .await
        .expect("seal exact blob");
    let stored = storage
        .prepare_blob_object(&locator, authority, slot, &spool)
        .await
        .expect("prepare exact blob object");
    storage
        .create_blob_object_from_file(&stored, authority, &spool, &no_progress())
        .await
        .expect("create exact blob object");
    stored
}

async fn enqueue_delete(
    db: &Database,
    stored: &crate::blob::locator::StoredBlobRef,
    created_at: &str,
) {
    let stored = stored.clone();
    let created_at = created_at.to_string();
    db.call(move |conn| Database::enqueue_delete_on(conn, &stored, &created_at))
        .await
        .expect("enqueue exact blob deletion");
}

async fn enqueue_local_upload(
    db: &Database,
    blob_id: &str,
    bytes: &[u8],
    source_dir: &std::path::Path,
) {
    plant_blob_row(db, blob_id, false, bytes).await;
    let row = db
        .row_blob_ref("note_photos", blob_id)
        .await
        .expect("load exact Local row blob reference");
    let source_path = source_dir.join(blob_id);
    crate::local_blob::write_atomic(&source_path, bytes)
        .await
        .expect("write upload source");
    let root_id = format!("note-{blob_id}");
    db.call(move |conn| {
        Database::enqueue_upload_on(conn, "notes", &root_id, &row, &source_path, false, T0)
    })
    .await
    .expect("enqueue exact Local row upload");
}

async fn insert_local_blob_row(
    db: &Database,
    root_id: &str,
    row_id: &str,
    blob_id: &str,
    cloud_path: Option<&str>,
    bytes: &[u8],
) {
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    let root_id = root_id.to_string();
    let row_id = row_id.to_string();
    let blob_id = blob_id.to_string();
    let cloud_path = cloud_path.map(str::to_string);
    let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
    let hash = crate::sync::store_commit::ObjectHash::digest(bytes).to_string();
    db.call(move |conn| {
        StoreDatabase::run_internal_store_write_transaction_on(
            conn,
            &tables,
            None,
            write_id,
            |tx| {
                tx.execute(
                    "INSERT INTO notes
                     (id, title, body, shared, _updated_at, created_at)
                     VALUES (?1, 'blob root', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
                    [root_id.as_str()],
                )
                .map_err(DbError::from)?;
                tx.execute(
                    "INSERT INTO note_photos
                     (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at)
                     VALUES (?1, ?2, 'cover', ?3, ?4, ?5, ?6,
                             '0000000001000-0000-dev1', '2026-01-01')",
                    rusqlite::params![row_id, root_id, size, hash, cloud_path, blob_id],
                )
                .map_err(DbError::from)?;
                Ok(())
            },
        )
    })
    .await
    .expect("insert journaled Local blob row");
}

async fn publish_exact_remote_blob_binding(
    db: &Database,
    storage: &TestStore,
    store_dir: &StoreDir,
    root_id: &str,
    row_id: &str,
    bytes: &[u8],
) -> crate::blob::locator::StoredBlobRef {
    let local = db
        .row_blob_ref("note_photos", row_id)
        .await
        .expect("load exact Local row blob reference");
    let source = store_dir
        .local_blob_path(&local.blob().namespace, &local.blob().id)
        .expect("resolve host blob source");
    crate::local_blob::write_atomic(&source, bytes)
        .await
        .expect("write host blob source");
    let hlc = crate::sync::hlc::Hlc::new("delete-tests".to_string());
    let database = StoreDatabase::new(db);
    crate::blob::transition::make_remote(&database, store_dir, &hlc, "notes", root_id, false)
        .await
        .expect("start exact make_remote");
    let clock = FixedClock(at("2024-06-01T01:00:00Z"));
    let (registration_ref, registration) = database
        .local_blob_write_authority()
        .await
        .expect("load exact blob upload authority");
    let authority = crate::sync::storage::BlobWriteAuthority::new(&registration_ref, &registration)
        .expect("validate exact blob upload authority");
    let outcome = crate::blob::upload::drain_uploads(
        &database,
        &storage.storage,
        authority,
        store_dir,
        &clock,
        &hlc,
        None,
        None,
    )
    .await
    .expect("drain exact blob upload");
    assert_eq!(outcome.uploaded, 1);
    assert!(storage
        .publish_pending(db, store_dir)
        .await
        .expect("publish exact remote blob binding"));
    db.row_blob_ref("note_photos", row_id)
        .await
        .expect("load exact Remote row blob reference")
        .stored()
        .cloned()
        .expect("Remote row owns an exact stored blob reference")
}

fn exact_tombstone_key(stored: &crate::blob::locator::StoredBlobRef) -> String {
    format!(
        "blob_tombstones/{}",
        crate::sync::remote_object::remote_object_id(stored.object())
    )
}

/// Exact test storage holding a real two-member chain: `founder` (Owner) added
/// `member` (Member). Returns the storage plus both keypairs so tests can sign as
/// a member, a non-member, or the founder.
async fn storage_with_chain(db: &Database) -> (TestStore, UserKeypair, UserKeypair) {
    let founder = UserKeypair::generate();
    let member = UserKeypair::generate();
    let storage = TestStore::create(db, "test-store", founder.clone())
        .await
        .expect("create exact Store membership fixture");
    crate::sync::store::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("founder".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &crate::encryption::EncryptionService::from_key([42; 32]),
        "test-store",
        "Test Store",
        &StoreDatabase::new(db),
    )
    .await
    .expect("publish exact member invitation");

    (storage, founder, member)
}

/// Write a tombstone object straight into the cloud at its key, bypassing the
/// signing drain — so a test can plant a forged or store-mismatched tombstone
/// the GC must reject. Mirrors how `drain_tombstones` lays the object out
/// (plaintext cipher → verbatim bytes, empty suffix).
async fn plant_tombstone(cloud: &dyn CloudHome, tombstone: &BlobTombstoneJson) {
    let key = exact_tombstone_key(&tombstone.stored);
    let bytes = serde_json::to_vec(tombstone).expect("serialize tombstone");
    cloud
        .write(
            &key,
            crate::storage::cloud::BlobBody::from_bytes(bytes),
            &no_progress(),
        )
        .await
        .expect("plant");
}

/// A `CloudHome` that delegates to an inner `TestStore`, but the first time
/// the GC re-checks the named tombstone key with `exists`, it simulates a
/// concurrent tombstone removal landing in the TOCTOU window: it deletes the
/// tombstone from the inner store and reports `false`. This drives the GC's
/// re-check-before-delete deterministically — the blob must then be left alone.
struct CancelTombstoneOnExists<'a> {
    inner: &'a dyn CloudHome,
    tombstone_key: String,
    fired: AtomicBool,
}

#[async_trait]
impl CloudHome for CancelTombstoneOnExists<'_> {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'b>(
        &'b self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'b>, CloudHomeError> {
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
        // The GC's pre-delete re-check: on the first hit for the tombstone key,
        // Remove it and report it gone. Any later/other check delegates normally.
        if key == self.tombstone_key && !self.fired.swap(true, Ordering::SeqCst) {
            self.inner.delete(key).await?;
            return Ok(false);
        }
        self.inner.exists(key).await
    }

    async fn set_access(
        &self,
        desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, CloudHomeError> {
        self.inner.set_access(desired).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailingCloudOp {
    Delete,
    Exists,
    PutObject,
    Read,
}

/// A `CloudHome` wrapper that fails one operation on one named key for the first
/// `fail_times` matching calls, counts matching calls, then delegates normally.
struct FailCloudOpOnKey<'a, H: CloudHome + ?Sized> {
    inner: &'a H,
    op: FailingCloudOp,
    key: String,
    fail_times: usize,
    calls: AtomicUsize,
}

impl<'a, H: CloudHome + ?Sized> FailCloudOpOnKey<'a, H> {
    fn new(inner: &'a H, op: FailingCloudOp, key: &str, fail_times: usize) -> Self {
        Self {
            inner,
            op,
            key: key.to_string(),
            fail_times,
            calls: AtomicUsize::new(0),
        }
    }

    fn matching_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn should_fail(&self, op: FailingCloudOp, key: &str) -> bool {
        if self.op == op && key == self.key {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            return call < self.fail_times;
        }
        false
    }
}

#[async_trait]
impl<H: CloudHome + ?Sized> CloudHome for FailCloudOpOnKey<'_, H> {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        if self.should_fail(FailingCloudOp::PutObject, key) {
            return Err(CloudHomeError::Transport(format!(
                "injected put_object failure for {key}"
            )));
        }
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'b>(
        &'b self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'b>, CloudHomeError> {
        self.inner.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        if self.should_fail(FailingCloudOp::Read, key) {
            return Err(CloudHomeError::Transport(format!(
                "injected read failure for {key}"
            )));
        }
        self.inner.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        if self.should_fail(FailingCloudOp::Delete, key) {
            return Err(CloudHomeError::Transport(format!(
                "injected delete failure for {key}"
            )));
        }
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        if self.should_fail(FailingCloudOp::Exists, key) {
            return Err(CloudHomeError::Transport(format!(
                "injected exists failure for {key}"
            )));
        }
        self.inner.exists(key).await
    }

    async fn set_access(
        &self,
        desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, CloudHomeError> {
        self.inner.set_access(desired).await
    }
}

// ----- the outbox Delete row becomes a tombstone and is removed -----

/// A queued blob delete drains to a signed cloud tombstone (the deletion's durable
/// record) and the outbox row is cleared — and crucially the blob is NOT deleted
/// yet (it is kept for the convergence grace).
#[tokio::test]
async fn enqueued_delete_becomes_a_tombstone_and_clears_the_outbox() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let cloud = storage.home.as_ref();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = create_exact_blob(&storage, "delete-tests", "queued-delete", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deletes_before = cloud.deletes_seen();

    enqueue_delete(&db, &stored, T0).await;

    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_tombstones(
        &db,
        cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &clock,
    )
    .await
    .expect("drain");
    assert_eq!(n, 1, "one tombstone written");

    // The blob is still present — the drain records the deletion, it doesn't
    // perform it.
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob is kept for the grace, not deleted on drain",
    );
    assert_eq!(
        cloud.deletes_seen(),
        deletes_before,
        "the drain deletes no provider object",
    );

    // A signed tombstone landed at the derived key and verifies under the store.
    let tombstone_bytes = cloud.get(&tombstone_key).expect("tombstone object present");
    let tombstone: BlobTombstoneJson =
        serde_json::from_slice(&tombstone_bytes).expect("parse tombstone");
    assert_eq!(tombstone.stored, stored);
    assert_eq!(tombstone.deleted_at, "2024-06-10T00:00:00+00:00");
    assert_eq!(tombstone.author_pubkey, hex::encode(kp.public_key()));
    assert!(tombstone.verify("lib"), "the tombstone is validly signed");

    // The outbox row is gone.
    assert!(
        db.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the drained delete row is removed",
    );
}

#[tokio::test]
async fn garbage_at_the_tombstone_key_does_not_clear_the_delete() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let cloud = storage.home.as_ref();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = create_exact_blob(&storage, "delete-tests", "garbage-slot", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    cloud
        .write(
            &tombstone_key,
            crate::storage::cloud::BlobBody::from_bytes(b"not a tombstone".to_vec()),
            &no_progress(),
        )
        .await
        .expect("plant garbage");

    enqueue_delete(&db, &stored, T0).await;
    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_tombstones(
        &db,
        cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &clock,
    )
    .await
    .expect("drain");

    assert_eq!(n, 1, "the garbage object is replaced by a signed tombstone");
    let tombstone_bytes = cloud.get(&tombstone_key).expect("tombstone object present");
    let tombstone: BlobTombstoneJson =
        serde_json::from_slice(&tombstone_bytes).expect("parse tombstone");
    assert_eq!(tombstone.stored, stored);
    assert!(tombstone.verify("lib"));
    assert!(
        db.get_pending_cloud_deletes().await.unwrap().is_empty(),
        "the delete row clears only after a valid tombstone is present",
    );
}

#[tokio::test]
async fn valid_existing_tombstone_is_preserved_by_delete_drain() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let cloud = storage.home.as_ref();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = create_exact_blob(&storage, "delete-tests", "existing", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let original = BlobTombstoneJson::signed(
        "lib",
        stored.clone(),
        "2024-06-01T00:00:00+00:00".to_string(),
        &kp,
    );
    plant_tombstone(cloud, &original).await;
    let original_bytes = cloud.get(&tombstone_key).expect("original tombstone");

    enqueue_delete(&db, &stored, T0).await;
    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_tombstones(
        &db,
        cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &clock,
    )
    .await
    .expect("drain");

    assert_eq!(n, 0, "an existing valid tombstone is not rewritten");
    assert_eq!(
        cloud.get(&tombstone_key),
        Some(original_bytes),
        "the existing tombstone remains byte-for-byte unchanged",
    );
    assert!(
        db.get_pending_cloud_deletes().await.unwrap().is_empty(),
        "the delete row clears because the valid tombstone is already present",
    );
}

/// An upload row reads back as an `Upload` carrying its scope; a delete row reads
/// back as a `Delete`. The operation-specific fields live in the variant, so a
/// delete has no scope to be `None` — the shared `cloud_outbox` row-shape
/// contract.
#[tokio::test]
async fn upload_carries_scope_delete_carries_no_extra_fields() {
    use crate::db::{OutboxOperation, OutboxUploadState};

    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    plant_blob_row(&db, "upload-row", false, b"upload body").await;
    let row = db
        .row_blob_ref("note_photos", "upload-row")
        .await
        .expect("load exact upload row");
    let source_dir = tempfile::tempdir().expect("create upload source directory");
    let source_path = source_dir.path().join("upload-row");
    crate::local_blob::write_atomic(&source_path, b"upload body")
        .await
        .expect("write upload source");
    let enqueue_row = row.clone();
    let enqueue_path = source_path.clone();
    db.call(move |conn| {
        Database::enqueue_upload_on(
            conn,
            "notes",
            "note-upload-row",
            &enqueue_row,
            &enqueue_path,
            false,
            T0,
        )
    })
    .await
    .expect("enqueue exact upload");
    let storage = TestStore::new().await;
    let stored = create_exact_blob(&storage, "delete-tests", "shape-delete", b"delete").await;
    enqueue_delete(&db, &stored, T0).await;

    let uploads = db.get_pending_cloud_uploads().await.expect("uploads");
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        uploads[0].operation,
        OutboxOperation::Upload {
            root_table: "notes".to_string(),
            root_id: "note-upload-row".to_string(),
            row,
            source_path,
            retain_pinned: false,
            state: OutboxUploadState::Pending,
        },
        "an upload entry carries its scope in the variant"
    );

    let deletes = db.get_pending_cloud_deletes().await.expect("deletes");
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].operation, OutboxOperation::Delete { stored });
}

/// A failed tombstone validation records durable retry state, then the
/// delete drain skips the row inside the backoff window and retries once the
/// window has elapsed.
#[tokio::test]
async fn delete_validation_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let stored = create_exact_blob(&storage, "delete-tests", "validation-retry", b"body").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let inner = storage.home.as_ref();
    let cloud = FailCloudOpOnKey::new(inner, FailingCloudOp::Read, &tombstone_key, 1);
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    enqueue_delete(&db, &stored, T0).await;
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let error = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &first,
    )
    .await
    .expect_err("failed validation fails the drain");
    assert!(error.contains("Failed to validate"), "{error}");
    assert_eq!(cloud.matching_calls(), 1);

    let first_row = get_delete(&db, 1).await.expect("delete row remains");
    assert_eq!(first_row.0, 1, "the failed attempt is counted");
    assert!(
        first_row
            .1
            .as_deref()
            .unwrap()
            .contains("tombstone validation failed"),
        "the failure reason is recorded",
    );
    let recorded = chrono::DateTime::parse_from_rfc3339(first_row.2.as_deref().unwrap()).unwrap();
    assert_eq!(recorded.with_timezone(&chrono::Utc), first.0);

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let n = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &inside,
    )
    .await
    .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        cloud.matching_calls(),
        1,
        "inside the backoff window no cloud validation runs",
    );
    assert_eq!(
        get_delete(&db, 1).await.expect("delete row remains"),
        first_row,
        "the skipped row is unchanged",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &after,
    )
    .await
    .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(cloud.matching_calls(), 2);
    assert!(
        inner.get(&tombstone_key).is_some(),
        "the retried drain writes the tombstone",
    );
    assert!(
        get_delete(&db, 1).await.is_none(),
        "the successful retry clears the delete row",
    );
}

/// A failed tombstone write records the same durable retry state as an existence
/// check failure, and the backoff gate suppresses the write attempt until the
/// retry window has elapsed.
#[tokio::test]
async fn delete_write_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let stored = create_exact_blob(&storage, "delete-tests", "write-retry", b"body").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let inner = storage.home.as_ref();
    let cloud = FailCloudOpOnKey::new(inner, FailingCloudOp::PutObject, &tombstone_key, 1);
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    enqueue_delete(&db, &stored, T0).await;
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let error = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &first,
    )
    .await
    .expect_err("failed write fails the drain");
    assert!(error.contains("Tombstone write failed"), "{error}");
    assert_eq!(cloud.matching_calls(), 1);

    let first_row = get_delete(&db, 1).await.expect("delete row remains");
    assert_eq!(first_row.0, 1, "the failed write is counted");
    assert!(
        first_row
            .1
            .as_deref()
            .unwrap()
            .contains("tombstone write failed"),
        "the write failure is recorded",
    );

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let n = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &inside,
    )
    .await
    .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        cloud.matching_calls(),
        1,
        "inside the backoff window no cloud write runs",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &after,
    )
    .await
    .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(cloud.matching_calls(), 2);
    assert!(
        inner.get(&tombstone_key).is_some(),
        "the retried drain writes the tombstone",
    );
}

/// Corrupt local retry metadata is not a retry decision. The drain surfaces the
/// invalid timestamp and leaves the delete row unchanged.
#[tokio::test]
async fn corrupt_delete_backoff_timestamp_fails_loud() {
    tokio::task::LocalSet::new()
        .run_until(async {
            tokio::task::spawn_local(async {
                Box::pin(corrupt_delete_backoff_timestamp_case()).await
            })
            .await
            .expect("corrupt timestamp case task completes");
        })
        .await;
}

async fn corrupt_delete_backoff_timestamp_case() {
    let db = open_outbox_db();
    let storage = Box::pin(TestStore::new()).await;
    let stored = Box::pin(create_exact_blob(
        &storage,
        "delete-tests",
        "corrupt-retry",
        b"body",
    ))
    .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let inner = storage.home.as_ref();
    let cloud = FailCloudOpOnKey::new(inner, FailingCloudOp::Read, &tombstone_key, 1);
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    Box::pin(enqueue_delete(&db, &stored, T0)).await;
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    Box::pin(drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &first,
    ))
    .await
    .expect_err("provider read failure fails the first drain");

    Box::pin(db.call(|conn| {
        conn.execute(
            "UPDATE cloud_outbox SET last_attempt_at = 'not-a-timestamp' \
             WHERE id = 1 AND operation = 'delete'",
            [],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }))
    .await
    .expect("corrupt last_attempt_at");

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let error = Box::pin(drain_tombstones(
        &db,
        &cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &inside,
    ))
    .await
    .expect_err("corrupt timestamp must fail the drain");
    assert!(error.contains("invalid last_attempt_at"), "{error}");
    assert_eq!(cloud.matching_calls(), 1);
    assert!(
        Box::pin(get_delete(&db, 1)).await.is_some(),
        "the delete row remains until its retry metadata is repaired explicitly",
    );
}

// ----- the grace: kept before, reclaimed after -----

/// A blob with a valid, authorized tombstone survives a GC pass run inside the
/// convergence grace, and is deleted by one run after it: the blob is kept long
/// enough for a lagging peer to converge, then reclaimed once the grace has passed.
#[tokio::test]
async fn tombstone_is_reclaimed_only_after_the_grace() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    // The exact blob exists; a member tombstoned it at a known instant.
    let stored = create_exact_blob(&storage, "delete-tests", "grace", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // A GC one day later — well inside the 7-day grace — keeps the blob.
    let inside = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &inside,
    )
    .await
    .expect("gc inside grace");
    assert_eq!(n, 0, "nothing reclaimed inside the grace");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives a GC inside the grace",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_ok(),
        "the tombstone is kept inside the grace",
    );

    // A GC just past the grace reclaims the blob and the tombstone.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc past grace");
    assert_eq!(n, 1, "one blob reclaimed past the grace");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_err(),
        "the blob is deleted past the grace",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_err(),
        "the tombstone is deleted after reclaiming its blob",
    );
}

#[tokio::test]
async fn tombstone_gc_cancels_when_a_live_row_still_references_the_blob() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    insert_local_blob_row(&db, "n1", "bloblive", "bloblive", None, b"live contents").await;
    let stored = publish_exact_remote_blob_binding(
        &db,
        &storage,
        &store_dir,
        "n1",
        "bloblive",
        b"live contents",
    )
    .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc");

    assert_eq!(n, 0, "a live blob reference prevents reclaim");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the referenced blob remains in cloud",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_err(),
        "the stale tombstone is canceled",
    );
}

/// A replaced blob's cloud object is reclaimed, and the blob that replaced it is not.
///
/// GC resolves live references through the installed exact row binding. The old
/// object has no binding and is collected; the replacement's exact object remains
/// bound to the winning row version and is protected.
#[tokio::test]
async fn tombstone_gc_reclaims_a_replaced_blob_and_keeps_the_one_that_replaced_it() {
    let db = open_test_db_with_blob(
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
            .with_id_column("blob_id")
            .with_cloud_path_column("cloud_path"),
    );
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let replaced = create_exact_blob(&storage, "photos", "p1cover", b"old cover").await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    insert_local_blob_row(
        &db,
        "n1",
        "ph1",
        "p2cover",
        Some("n1/cover-p2cover.jpg"),
        b"live cover",
    )
    .await;
    let live =
        publish_exact_remote_blob_binding(&db, &storage, &store_dir, "n1", "ph1", b"live cover")
            .await;
    // The replacement tombstoned the blob it replaced. A tombstone also stands for the
    // live blob's key — a stale one the GC must cancel, not act on.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    for stored in [replaced.clone(), live.clone()] {
        let tombstone =
            BlobTombstoneJson::signed("test-lib", stored, deleted_at.to_string(), &member);
        plant_tombstone(storage.home.as_ref(), &tombstone).await;
    }

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let reclaimed = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc");

    assert_eq!(reclaimed, 1, "exactly the replaced blob is reclaimed");
    assert!(
        storage.storage.verify_blob_object(&replaced).await.is_err(),
        "the replaced blob's object is collected — no live row names its key",
    );
    assert!(
        storage.storage.verify_blob_object(&live).await.is_ok(),
        "the blob the row now holds is protected by the live-row check",
    );
    assert!(
        storage
            .home
            .read(&exact_tombstone_key(&live))
            .await
            .is_err(),
        "the stale tombstone over the live blob is canceled",
    );
}

/// A lagging peer's cycle is pull→GC. If it pulled just before the writer pushed the
/// retraction but ran GC just after the tombstone was written, its db still reads the
/// blob's row live+remote while the tombstone is fresh. Within the grace the tombstone
/// must survive that stale row state: the writer's outbox row is already gone, so a
/// cancel here would strand the cloud blob forever. Once the peer pulls the retraction
/// and the grace passes, the blob is reclaimed.
#[tokio::test]
async fn tombstone_within_grace_survives_gc_despite_a_stale_live_row() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    insert_local_blob_row(&db, "n1", "bloblive", "bloblive", None, b"live contents").await;
    let stored = publish_exact_remote_blob_binding(
        &db,
        &storage,
        &store_dir,
        "n1",
        "bloblive",
        b"live contents",
    )
    .await;
    let tombstone_key = exact_tombstone_key(&stored);

    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // GC inside the grace, with the row still reading live+remote: the fresh tombstone
    // must NOT be canceled.
    let within = FixedClock(at(deleted_at));
    let n = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &within,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc within grace");
    assert_eq!(n, 0, "nothing reclaimed inside the grace");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives inside the grace",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_ok(),
        "the tombstone survives inside the grace despite the stale live row",
    );

    // The peer pulls the retraction (the row is gone); past grace the blob is reclaimed.
    exec(
        &db,
        "DELETE FROM note_photos WHERE id = 'bloblive'; DELETE FROM notes WHERE id = 'n1'",
    )
    .await;
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc past grace");
    assert_eq!(
        n, 1,
        "the blob is reclaimed once grace passes and the row is gone"
    );
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_err(),
        "the blob is deleted past the grace",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_err(),
        "the tombstone is deleted after reclaiming its blob",
    );
}

/// A blob-bearing row whose FK parent is missing resolves to no locality terminus. GC
/// must neither cancel the tombstone nor reclaim the blob on that unresolved state — it
/// skips loudly and leaves both in place for a later pass.
#[tokio::test]
async fn tombstone_gc_fails_when_the_referencing_row_locality_is_unresolved() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    insert_local_blob_row(&db, "n1", "orphan", "orphan", None, b"orphan contents").await;
    let stored = publish_exact_remote_blob_binding(
        &db,
        &storage,
        &store_dir,
        "n1",
        "orphan",
        b"orphan contents",
    )
    .await;
    let tombstone_key = exact_tombstone_key(&stored);

    // Remove the bound row's parent with foreign keys disabled. The exact child
    // binding remains, but locality resolution reaches no root row.
    exec(
        &db,
        "PRAGMA foreign_keys=OFF; \
         DELETE FROM notes WHERE id = 'n1'; \
         PRAGMA foreign_keys=ON",
    )
    .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let error = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect_err("unresolved locality fails GC");

    assert!(error.contains("locality is unresolved"), "{error}");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob is untouched when locality is unresolved",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_ok(),
        "the tombstone is kept when locality is unresolved",
    );
}

/// A member's GC physically deletes only blobs under its own `{namespace}/{self}/`
/// prefix. A past-grace, authorized tombstone for a blob under *another* member's
/// prefix is left standing — that member's own GC, or an owner sweep, reclaims it.
/// Here a plain member reclaims its own blob and leaves a peer's untouched.
#[tokio::test]
async fn gc_reclaims_own_prefix_and_leaves_a_foreign_members_blob() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let member_db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    crate::sync::test_helpers::install_active_device_fixture(
        &storage,
        &db,
        &member_db,
        &member,
        "2024-06-01T00:00:00Z",
    )
    .await
    .expect("activate exact member uploader");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    crate::sync::test_helpers::pull_into(&db, &storage, &store_dir).await;

    // One blob under the member's own prefix, one under the founder's. No live rows
    // reference either (the DB is empty), so both are ripe for reclaim — only the
    // prefix gate decides which the member may delete.
    let mine =
        create_exact_blob_as(&storage, &member_db, &member, "photos", "mineblob", b"mine").await;
    let foreign = create_exact_blob(&storage, "photos", "foreignblob", b"foreign").await;
    assert_eq!(
        StoreDatabase::new(&member_db)
            .activated_store_device_registration(mine.locator().uploader().clone())
            .await
            .expect("member uploader activation is visible to GC")
            .author_pubkey,
        pubkey_hex(&member),
    );
    let deleted_at = "2024-06-01T00:00:00+00:00";
    for stored in [mine.clone(), foreign.clone()] {
        let tombstone =
            BlobTombstoneJson::signed("test-lib", stored, deleted_at.to_string(), &member);
        plant_tombstone(storage.home.as_ref(), &tombstone).await;
    }

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_as(
        &pubkey_hex(&member),
        &member_db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc");

    assert_eq!(n, 1, "the member reclaims exactly its own-prefix blob");
    assert!(
        storage.storage.verify_blob_object(&mine).await.is_err(),
        "the member's own-prefix blob is reclaimed",
    );
    assert!(
        storage.storage.verify_blob_object(&foreign).await.is_ok(),
        "a blob under another member's prefix is left for its owner or an owner sweep",
    );
    assert!(
        storage
            .home
            .read(&exact_tombstone_key(&foreign))
            .await
            .is_ok(),
        "the foreign blob's tombstone is kept so its uploader or an owner can still act",
    );
}

/// An owner sweeps other members' prefixes: it retains bucket-wide delete, so a
/// past-grace tombstone for an absent member's blob is reclaimed by the owner even
/// though the object sits under that member's prefix.
#[tokio::test]
async fn owner_sweep_reclaims_an_absent_members_blob() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (storage, founder, member) = storage_with_chain(&db).await;
    let owner = pubkey_hex(&founder);
    let cipher = plaintext_cipher();
    let member_db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    crate::sync::test_helpers::install_active_device_fixture(
        &storage,
        &db,
        &member_db,
        &member,
        "2024-06-01T00:00:00Z",
    )
    .await
    .expect("activate exact member uploader");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    crate::sync::test_helpers::pull_into(&db, &storage, &store_dir).await;

    let foreign = create_exact_blob_as(
        &storage,
        &member_db,
        &member,
        "photos",
        "absentblob",
        b"contents",
    )
    .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", foreign.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_as(
        &owner,
        &member_db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        BLOB_TOMBSTONE_GRACE,
    )
    .await
    .expect("gc");

    assert_eq!(
        n, 1,
        "the owner reclaims the absent member's condemned blob"
    );
    assert!(
        storage.storage.verify_blob_object(&foreign).await.is_err(),
        "an owner sweep deletes a blob under another member's prefix",
    );
}

/// An instant one minute past `deleted_at + BLOB_TOMBSTONE_GRACE`, as RFC 3339.
fn past_grace_instant(deleted_at: &str) -> String {
    (at(deleted_at) + BLOB_TOMBSTONE_GRACE + chrono::Duration::minutes(1)).to_rfc3339()
}

/// A tombstone removed between GC verifying/aging it and deleting the blob must
/// not authorize the delete. GC re-checks the tombstone immediately before the
/// exact-object deletion and skips when it is gone.
#[tokio::test]
async fn tombstone_removed_mid_gc_leaves_the_exact_blob() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    // Authorization passes, so GC reaches the pre-delete re-check where the
    // simulated concurrent removal lands.
    let stored = create_exact_blob(&storage, "delete-tests", "cancel-mid-gc", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // The cloud home removes the tombstone in GC's re-check window.
    let racing_home = CancelTombstoneOnExists {
        inner: storage.home.as_ref(),
        tombstone_key,
        fired: AtomicBool::new(false),
    };

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n =
        gc_tombstones_without_live_refs(&db, &storage, &racing_home, &cipher, "test-lib", &past)
            .await
            .expect("gc");
    assert_eq!(n, 0, "a tombstone removed mid-GC reclaims nothing");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the exact blob survives after its tombstone disappears",
    );
}

/// Past the grace, with no conditional-delete support on the provider, a blob is
/// erased with a plain delete; the tombstone is a mock backed only by the
/// in-memory store's `delete`/`exists` (no `object_state`/`delete_if_version`).
#[tokio::test]
async fn past_grace_tombstone_erases_the_blob_on_a_plain_delete_provider() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    let stored = create_exact_blob(&storage, "delete-tests", "plain-delete", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc past grace");

    assert_eq!(n, 1, "the blob is erased past the grace");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_err(),
        "the blob is gone after a plain-delete reclaim",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_err(),
        "the tombstone is removed after reclaiming its blob",
    );
}

/// A grace the host configures (here one hour) is what the GC ages against, not
/// the seven-day default: within the hour the blob survives; past it the blob is
/// erased. The reader evaluates whatever grace it is handed.
#[tokio::test]
async fn a_configured_one_hour_grace_is_honored() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let grace = chrono::Duration::hours(1);

    let stored = create_exact_blob(&storage, "delete-tests", "configured-grace", b"contents").await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // Half an hour in — inside the configured hour — the blob survives.
    let within = FixedClock(at("2024-06-01T00:30:00Z"));
    let n = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &within,
        grace,
    )
    .await
    .expect("gc within the configured hour");
    assert_eq!(n, 0, "nothing reclaimed inside the configured hour");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives inside the configured hour (well before the 7-day default)",
    );

    // Just past the hour — past the configured grace though far inside the default —
    // the blob is erased.
    let past = FixedClock(at("2024-06-01T01:00:01Z"));
    let n = gc_tombstones_anchored(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
        grace,
    )
    .await
    .expect("gc past the configured hour");
    assert_eq!(n, 1, "the blob is erased once the configured hour passes");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_err(),
        "the blob is gone past the configured grace",
    );
}

// ----- forgery: a bad signature or a non-member author is ignored -----

/// A tombstone whose author is NOT a current member is ignored by the GC: the blob
/// survives. This is the forgery defense — a bucket writer who isn't a member
/// can't delete a blob by planting a (validly self-signed) tombstone.
#[tokio::test]
async fn tombstone_by_a_non_member_is_ignored() {
    let db = open_test_db();
    let (storage, _founder, _member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let outsider = UserKeypair::generate(); // not in the chain

    let stored = create_exact_blob(&storage, "delete-tests", "non-member", b"contents").await;
    // Validly signed by the outsider (the signature itself verifies), but the
    // outsider is not a member — long past the grace, so only authorization stands
    // between this and a deletion.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        stored.clone(),
        deleted_at.to_string(),
        &outsider,
    );
    assert!(
        tombstone.verify("test-lib"),
        "the forged tombstone is self-consistently signed (only authorization rejects it)",
    );
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(n, 0, "a non-member tombstone reclaims nothing");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives a non-member's forged deletion",
    );
}

/// The owner-anchoring defense: a tombstone authored by the *forged founder of a
/// wiped-and-refounded chain* is refused, so the victim blob survives — even though
/// the attacker controls `membership/*` and self-signed a founder entry that passes
/// `MembershipChain::validate`.
///
/// An attacker who can write the bucket wipes `membership/*`, writes a fresh
/// self-signed Owner founder under their own key (internally valid — `validate`
/// only proves consistency, not that the founder is the store's established
/// owner), then plants a tombstone signed by that forged founder with a backdated
/// `deleted_at`. Because the device pins the *real* owner, the refounded chain
/// fails `is_founded_by(pinned_owner)`, the author is never authorized, and the
/// backdated time never gets a chance to matter.
#[tokio::test]
async fn tombstone_by_a_forged_founder_of_a_refounded_chain_is_refused() {
    // The store's real established owner (the pinned founder). Its chain isn't
    // even needed for the test — the device only needs to *pin* this pubkey; the
    // attacker has replaced the on-bucket chain entirely.
    let real_owner = UserKeypair::generate();

    // The attacker controls the bucket. They wrote a forged self-signed Owner
    // founder under their OWN key — a valid chain in isolation, but founded by the
    // wrong key.
    let attacker = UserKeypair::generate();
    let attacker_db = open_test_db();
    let storage = TestStore::create(&attacker_db, "test-store", attacker.clone())
        .await
        .expect("create attacker-founded exact Store");
    // The victim blob, and a tombstone the attacker signs as their forged founder,
    // backdated well past the grace so only authorization stands between it and the
    // deletion.
    let stored = create_exact_blob(&storage, "delete-tests", "refounded", b"contents").await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        stored.clone(),
        deleted_at.to_string(),
        &attacker,
    );
    assert!(
        tombstone.verify("test-lib"),
        "the forged tombstone is self-consistently signed (only owner-anchored \
         authorization rejects it)",
    );
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // Opening this exact Store under the established owner refuses the forged root
    // before a cycle can load membership or run GC.
    let joining_db = open_test_db();
    let result = crate::sync::test_helpers::open_exact_test_store_as(
        &joining_db,
        &storage.storage,
        &storage.root,
        &real_owner,
    )
    .await;
    assert!(
        result.is_err(),
        "loading a chain refounded under a non-owner key refuses the cycle before \
         any tombstone is judged, got {result:?}",
    );
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the victim blob survives a wiped-and-refounded-chain takeover",
    );
}

/// The empty-chain half of the same defense: an *entirely wiped* `membership/*`
/// (no founder at all) under a pinned owner is a takeover, not an open store, so
/// a tombstone authored over it is refused and the blob survives. The rule is the
/// cycle's membership load's: empty + pinned owner = wiped = refuse; empty + no
/// pin = pre-initialization caller = accept on signature.
#[tokio::test]
async fn tombstone_over_a_wiped_chain_with_a_pinned_owner_is_refused() {
    // The store has an established owner (pinned), but `membership/*` is wiped —
    // the storage's membership listing is empty.
    let real_owner = UserKeypair::generate();
    let founder_db = open_test_db();
    let storage = TestStore::create(&founder_db, "test-store", UserKeypair::generate())
        .await
        .expect("create exact Store before wiping its membership head");
    let founder_graph = crate::sync::store::StoreDatabase::new(&founder_db)
        .local_store_founder_graph()
        .await
        .expect("load exact founder graph")
        .expect("created Store has a founder graph");
    let stored = create_exact_blob(&storage, "delete-tests", "wiped", b"contents").await;
    let crate::database::DurableFounderMembership { head, .. } = founder_graph.membership;
    storage
        .delete_protocol_object(&head.object)
        .await
        .expect("wipe exact founder membership head");

    // A blob and a validly self-signed tombstone by some attacker, backdated past
    // the grace.
    let attacker = UserKeypair::generate();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        stored.clone(),
        deleted_at.to_string(),
        &attacker,
    );
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let joining_db = open_test_db();
    let result = crate::sync::test_helpers::open_exact_test_store_as(
        &joining_db,
        &storage.storage,
        &storage.root,
        &real_owner,
    )
    .await;
    assert!(
        result.is_err(),
        "an empty (wiped) chain under a pinned owner refuses the cycle at load, \
         before any tombstone is judged, got {result:?}",
    );
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives a tombstone over a wiped membership chain",
    );
}

/// A tombstone whose signature does not verify is ignored by the GC: the blob
/// survives. Covers an exact stored reference changed after signing, so the
/// signature no longer matches — the bucket is untrusted, so an
/// unauthenticated tombstone must never delete data.
#[tokio::test]
async fn tombstone_with_a_bad_signature_is_ignored() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    let stored = create_exact_blob(&storage, "delete-tests", "bad-signature", b"contents").await;
    let other = create_exact_blob(&storage, "delete-tests", "signed-other", b"other").await;
    // A member signs a tombstone for another exact object, then its stored
    // reference is replaced. The signature no longer covers the object in its slot.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let mut tombstone =
        BlobTombstoneJson::signed("test-lib", other, deleted_at.to_string(), &member);
    tombstone.stored = stored.clone();
    assert!(
        !tombstone.verify("test-lib"),
        "the tampered tombstone does not verify",
    );
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(n, 0, "a tombstone with a bad signature reclaims nothing");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives a tombstone whose signature doesn't verify",
    );
}

// ----- a tombstone bound to a different store is ignored -----

/// A tombstone validly signed for a DIFFERENT store is ignored when GC'd as this
/// store: the blob survives. A member of two stores can't replay one
/// store's deletion against the other's bucket — the signature binds the store
/// id, so it fails to verify under any other.
#[tokio::test]
async fn tombstone_bound_to_a_different_store_is_ignored() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    let stored = create_exact_blob(&storage, "delete-tests", "foreign-store", b"contents").await;
    // Signed for "other-lib" by a real member of THIS store; the GC runs as
    // "test-lib", so the signature (taken over other-lib's id) fails here.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("other-lib", stored.clone(), deleted_at.to_string(), &member);
    assert!(
        tombstone.verify("other-lib") && !tombstone.verify("test-lib"),
        "the tombstone verifies only under the store it was signed for",
    );
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(n, 0, "a foreign-store tombstone reclaims nothing");
    assert!(
        storage.storage.verify_blob_object(&stored).await.is_ok(),
        "the blob survives a tombstone bound to a different store",
    );
}

// ----- exact upload and delete intents remain independent -----

/// Upload and delete intents refer to different identities. A delete owns one
/// exact immutable object; an upload owns one exact Local row version whose
/// provider object has not been allocated yet. Reusing a logical blob id cannot
/// cancel either intent.
#[tokio::test]
async fn enqueue_upload_and_delete_remain_independent_for_exact_objects() {
    let storage = TestStore::new().await;
    let deleted = create_exact_blob(&storage, "photos", "same-id", b"old").await;

    // Delete then upload: both exact intents remain.
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let sources = tempfile::tempdir().expect("upload sources");
    enqueue_delete(&db, &deleted, T0).await;
    enqueue_local_upload(&db, "same-id", b"replacement", sources.path()).await;
    assert_eq!(
        db.get_pending_cloud_uploads().await.unwrap().len(),
        1,
        "the exact row-version upload remains",
    );
    assert_eq!(
        db.get_pending_cloud_deletes().await.unwrap().len(),
        1,
        "the old exact-object deletion remains",
    );

    // Upload then delete has the same two independent rows.
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let sources = tempfile::tempdir().expect("upload sources");
    enqueue_local_upload(&db, "same-id", b"replacement", sources.path()).await;
    enqueue_delete(&db, &deleted, T0).await;
    assert_eq!(
        db.get_pending_cloud_uploads().await.unwrap().len(),
        1,
        "the exact row-version upload remains",
    );
    assert_eq!(
        db.get_pending_cloud_deletes().await.unwrap().len(),
        1,
        "the old exact-object deletion remains",
    );

    // A different logical id is equally independent.
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let sources = tempfile::tempdir().expect("upload sources");
    enqueue_local_upload(&db, "other-id", b"other", sources.path()).await;
    enqueue_delete(&db, &deleted, T0).await;
    assert_eq!(
        db.get_pending_cloud_uploads().await.unwrap().len(),
        1,
        "the unrelated exact upload remains",
    );
    assert_eq!(
        db.get_pending_cloud_deletes().await.unwrap().len(),
        1,
        "the old exact-object deletion remains",
    );
}

/// A prior-cycle tombstone remains bound to its exact old object when the same
/// logical blob is uploaded again. The upload drain allocates another object;
/// GC later reclaims only the old one.
#[tokio::test]
async fn reupload_through_the_drain_preserves_the_old_exact_tombstone() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let tmp = tempfile::tempdir().unwrap();

    let old = create_exact_blob(&storage, "photos", "blob-key", b"old contents").await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", old.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    insert_local_blob_row(
        &db,
        "note-blob-key",
        "blob-key",
        "blob-key",
        None,
        b"fresh contents",
    )
    .await;
    let store_dir = StoreDir::new(tmp.path());
    let replacement = publish_exact_remote_blob_binding(
        &db,
        &storage,
        &store_dir,
        "note-blob-key",
        "blob-key",
        b"fresh contents",
    )
    .await;
    assert_ne!(old.object(), replacement.object());
    assert!(storage.storage.verify_blob_object(&old).await.is_ok());
    assert!(storage
        .storage
        .verify_blob_object(&replacement)
        .await
        .is_ok());
    assert!(storage.home.read(&exact_tombstone_key(&old)).await.is_ok());

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let gc = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(gc, 1, "the old exact object is reclaimed");
    assert!(storage.storage.verify_blob_object(&old).await.is_err());
    storage
        .storage
        .verify_blob_object(&replacement)
        .await
        .expect("the replacement exact object survives the old tombstone");
}

// ----- the tombstone write is idempotent: a re-drain holds the grace deadline -----

/// A Delete row drained when a tombstone already exists for its key does not
/// overwrite the tombstone: it keeps the original `deleted_at`, so the 7-day grace
/// is measured from the first drain and never resets. The row is processed twice
/// here — the second time at a clock a day later, standing in for the row surviving
/// a failed removal and re-draining (or the host re-enqueuing the same deletion).
/// If the second drain reset `deleted_at`, a row that keeps re-draining would push
/// the reclaim deadline forward forever and the blob would never age out.
#[tokio::test]
async fn re_draining_a_delete_keeps_the_original_deleted_at() {
    let db = open_outbox_db();
    let storage = TestStore::new().await;
    let cloud = storage.home.as_ref();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = create_exact_blob(&storage, "delete-tests", "redrain", b"contents").await;
    let tombstone_key = exact_tombstone_key(&stored);

    // First drain at T0: writes the tombstone, removes the row.
    enqueue_delete(&db, &stored, T0).await;
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let n = drain_tombstones(
        &db,
        cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &first,
    )
    .await
    .expect("first drain");
    assert_eq!(n, 1, "the first drain writes the tombstone");
    let original_deleted_at = {
        let bytes = cloud.get(&tombstone_key).expect("tombstone");
        let t: BlobTombstoneJson = serde_json::from_slice(&bytes).unwrap();
        t.deleted_at
    };

    // The same deletion is queued again (its row re-appears: the prior removal
    // failed, or the host re-enqueued it) and drained a day later. The tombstone
    // already exists, so this drain must not rewrite it.
    enqueue_delete(&db, &stored, "2024-06-02T00:00:00Z").await;
    let second = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = drain_tombstones(
        &db,
        cloud,
        &cipher,
        &PendingRotation::none(),
        "lib",
        &kp,
        &second,
    )
    .await
    .expect("second drain");
    assert_eq!(n, 0, "the re-drain writes no new tombstone");

    // Exactly one tombstone, still carrying the first drain's deleted_at.
    let tombstones = cloud.list("blob_tombstones/").await.unwrap();
    assert_eq!(tombstones.len(), 1, "only one tombstone exists for the key");
    let bytes = cloud.get(&tombstone_key).expect("tombstone");
    let tombstone: BlobTombstoneJson = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        tombstone.deleted_at, original_deleted_at,
        "the re-drain preserves the original deleted_at — the grace is not reset",
    );

    // The re-enqueued row is removed too (the drain always removes the row once the
    // tombstone is present), so the queue converges.
    assert!(
        db.get_pending_cloud_deletes().await.unwrap().is_empty(),
        "the re-drained row is removed",
    );
}

// ----- exact immutable replacements need no tombstone-cancel queue -----

/// A tombstone names one exact provider object. Re-uploading the same logical blob
/// allocates another exact object, so collecting the old object cannot erase the
/// replacement even though both share one locator.
#[tokio::test]
async fn old_tombstone_reclaims_only_the_exact_replaced_object() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let old = create_exact_blob(&storage, "photos", "blob", b"same bytes").await;
    let replacement = create_exact_blob(&storage, "photos", "blob", b"same bytes").await;
    assert_ne!(
        old.object(),
        replacement.object(),
        "a replacement owns a distinct exact provider object",
    );
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", old.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let reclaimed = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("reclaim the exact replaced object");
    assert_eq!(reclaimed, 1);
    assert!(storage.verify_blob_object(&old).await.is_err());
    storage
        .verify_blob_object(&replacement)
        .await
        .expect("replacement exact object survives the old tombstone");
}

/// A failed exact-object delete leaves the signed tombstone in place. A later GC
/// retries the same exact reference and removes both objects after the provider
/// accepts the delete.
#[tokio::test]
async fn exact_blob_delete_failure_leaves_tombstone_for_retry() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let stored = create_exact_blob(&storage, "photos", "retry", b"retry bytes").await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", stored.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;
    storage.home.fail_exact_delete_on_call(1);
    let past = FixedClock(at(&past_grace_instant(deleted_at)));

    let error = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect_err("exact delete failure fails GC");
    assert!(error.contains("Failed to delete blob"), "{error}");
    storage
        .verify_blob_object(&stored)
        .await
        .expect("failed exact delete leaves the blob present");
    assert!(storage
        .home
        .read(&exact_tombstone_key(&stored))
        .await
        .is_ok());

    let second = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("retry GC");
    assert_eq!(second, 1);
    assert!(storage.verify_blob_object(&stored).await.is_err());
    assert!(storage
        .home
        .read(&exact_tombstone_key(&stored))
        .await
        .is_err());
}

// ----- the GC tolerates a tombstone left over by a failed tombstone delete -----

/// When the GC reclaims a blob but the follow-up tombstone delete fails, the old
/// exact object is gone and the tombstone is left for a retry. A replacement with
/// the same logical blob id owns another exact object, so the retry removes only
/// the stale tombstone and cannot delete the replacement.
#[tokio::test]
async fn a_tombstone_left_by_a_failed_delete_is_harmless() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let (storage, _founder, member) = Box::pin(storage_with_chain(&db)).await;
    let cipher = plaintext_cipher();
    let old = create_exact_blob(&storage, "photos", "blob-key", b"contents").await;
    let tombstone_key = exact_tombstone_key(&old);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("test-lib", old.clone(), deleted_at.to_string(), &member);
    plant_tombstone(storage.home.as_ref(), &tombstone).await;

    // First GC past the grace: the blob delete succeeds, but the tombstone delete
    // fails (injected). The blob is reclaimed; the tombstone is left behind.
    let failing = FailCloudOpOnKey::new(
        storage.home.as_ref(),
        FailingCloudOp::Delete,
        &tombstone_key,
        1,
    );
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let error =
        gc_tombstones_without_live_refs(&db, &storage, &failing, &cipher, "test-lib", &past)
            .await
            .expect_err("tombstone delete failure fails GC");
    assert!(error.contains("Failed to delete tombstone"), "{error}");
    assert!(
        storage.storage.verify_blob_object(&old).await.is_err(),
        "the blob is deleted",
    );
    assert!(
        storage.home.read(&tombstone_key).await.is_ok(),
        "the tombstone is left behind because its delete failed",
    );

    // Upload a replacement while the stale exact tombstone remains.
    let tmp = tempfile::tempdir().unwrap();
    insert_local_blob_row(
        &db,
        "note-blob-key",
        "blob-key",
        "blob-key",
        None,
        b"re-uploaded contents",
    )
    .await;
    let store_dir = StoreDir::new(tmp.path());
    let replacement = publish_exact_remote_blob_binding(
        &db,
        &storage,
        &store_dir,
        "note-blob-key",
        "blob-key",
        b"re-uploaded contents",
    )
    .await;
    assert_ne!(old.object(), replacement.object());
    assert!(
        storage.home.read(&tombstone_key).await.is_ok(),
        "the replacement does not cancel a tombstone for another exact object",
    );

    // The next GC sees the old object already absent, removes its tombstone without
    // counting another reclaim, and leaves the replacement object intact.
    let n = gc_tombstones_without_live_refs(
        &db,
        &storage,
        storage.home.as_ref(),
        &cipher,
        "test-lib",
        &past,
    )
    .await
    .expect("gc after re-upload");
    assert_eq!(n, 0, "the absent old object is not counted twice");
    assert!(
        storage.home.read(&tombstone_key).await.is_err(),
        "the leftover tombstone is cleaned up",
    );
    assert!(
        storage
            .storage
            .verify_blob_object(&replacement)
            .await
            .is_ok(),
        "the re-uploaded blob survives the leftover-tombstone window",
    );
}
