//! Tests for the blob-delete half: signed tombstones, the graced GC that performs
//! the actual deletion, exact upload/delete intent independence, live-reference
//! tombstone cancellation, and the shared `cloud_outbox` row shape.
//!
//! The grace and forgery behaviors are the load-bearing ones — this code deletes
//! user data and trusts a signature, so a stale or forged tombstone is real data
//! loss.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::blob::delete::{BlobTombstoneJson, TombstoneDrain};
use crate::storage::{CloudCipher, PendingRotation, SyncStorage};
use crate::sync::test_helpers::{
    exact_tombstone_key, open_test_db, open_test_db_with_blob, open_test_db_with_tombstone_grace,
    plaintext_cipher, pubkey_hex, InterceptedStorage, ProviderObjectExistsInterception,
    StorageInterceptor, TestStore,
};
use crate::sync::test_owner_graph::TestOwnerGraph;
use coven_database::Database;
use coven_database::StoreDatabase;
use coven_foundation::clock::FixedClock;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::StorageError;
use coven_protocol::synced_schema::BlobDecl;

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
    let db = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &[],
    )
    .expect("open outbox database");
    db
}

async fn test_store() -> Arc<TestStore> {
    let database = open_test_db();
    TestStore::create(
        &database,
        "test-store",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact blob deletion test Store")
}

struct TombstoneCollector<'a> {
    store: crate::sync::store::Store,
    cipher: &'a RwLock<CloudCipher>,
}

impl<'a> TombstoneCollector<'a> {
    async fn load(
        database: StoreDatabase,
        fixture: &TestStore,
        storage: Arc<dyn SyncStorage>,
        cipher: &'a RwLock<CloudCipher>,
        identity: &UserKeypair,
    ) -> Result<Self, String> {
        let (_store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        Ok(Self {
            store: fixture
                .open_store_with_storage(database, storage, store_dir, identity)
                .await?,
            cipher,
        })
    }

    async fn for_founder(
        database: StoreDatabase,
        storage: &Arc<TestStore>,
        cipher: &'a RwLock<CloudCipher>,
    ) -> Result<Self, String> {
        Self::for_founder_with_storage(database, storage, storage.storage(), cipher).await
    }

    async fn for_founder_with_storage(
        database: StoreDatabase,
        fixture: &TestStore,
        storage: Arc<dyn SyncStorage>,
        cipher: &'a RwLock<CloudCipher>,
    ) -> Result<Self, String> {
        let (_store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        Ok(Self {
            store: fixture
                .open_founder_store_with_storage(database, storage, store_dir)
                .await?,
            cipher,
        })
    }

    async fn collect(&self, clock: &dyn coven_foundation::clock::Clock) -> Result<usize, String> {
        self.store
            .authorize_writer()
            .await
            .map_err(|error| error.to_string())?
            .gc_tombstones(self.cipher, clock)
            .await
    }
}

/// Exact test storage holding a real two-member chain: `founder` (Owner) added
/// `member` (Member). Returns the storage plus both keypairs so tests can sign as
/// a member, a non-member, or the founder.
async fn storage_with_chain(
    db: &Database,
) -> (std::sync::Arc<TestStore>, UserKeypair, UserKeypair) {
    let founder = UserKeypair::generate();
    let member = UserKeypair::generate();
    let storage = TestStore::create(
        db,
        "test-store",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store membership fixture");
    storage
        .invite_member(
            db,
            &founder,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .expect("publish exact member invitation");

    (storage, founder, member)
}

async fn tombstone_exists(storage: &TestStore, key: &str) -> bool {
    storage
        .storage()
        .provider_object_exists(key)
        .await
        .expect("inspect exact tombstone")
}

fn signed_store_tombstone(
    storage: &TestStore,
    stored: coven_protocol::blob::locator::StoredBlobRef,
    deleted_at: String,
    author: &UserKeypair,
) -> BlobTombstoneJson {
    BlobTombstoneJson::signed(
        &storage.root.store_root_id.to_string(),
        stored,
        deleted_at,
        author,
    )
}

/// A storage interceptor that, the first time
/// the GC re-checks the named tombstone key with `exists`, it simulates a
/// concurrent tombstone removal landing in the TOCTOU window: it deletes the
/// tombstone from the inner store and reports `false`. This drives the GC's
/// re-check-before-delete deterministically — the blob must then be left alone.
struct CancelTombstoneOnExists {
    tombstone_key: String,
    fired: AtomicBool,
}

/// A tombstone drain over the fixed parts every test here shares: no pending
/// rotation, one store id. Only the storage the drain writes through and the
/// clock it reads deletion times from vary between tests.
async fn drain_at(
    store_database: &StoreDatabase,
    storage: &dyn crate::storage::SyncStorage,
    cipher: &dyn crate::storage::CloudCipherAccess,
    keypair: &UserKeypair,
    clock: &dyn coven_foundation::clock::Clock,
) -> Result<usize, String> {
    TombstoneDrain::new(
        store_database,
        storage,
        cipher,
        &PendingRotation::none(),
        "lib",
        keypair,
        clock,
    )
    .drain()
    .await
}

#[async_trait]
impl StorageInterceptor for CancelTombstoneOnExists {
    async fn before_provider_object_exists(
        &self,
        key: &str,
    ) -> Result<ProviderObjectExistsInterception, StorageError> {
        // The GC's pre-delete re-check: on the first hit for the tombstone key,
        // remove it and report it gone. Any later/other check delegates normally.
        if key == self.tombstone_key && !self.fired.swap(true, Ordering::SeqCst) {
            return Ok(ProviderObjectExistsInterception::DeleteAndReportAbsent);
        }
        Ok(ProviderObjectExistsInterception::Proceed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailingCloudOp {
    Delete,
    Exists,
    PutObject,
    Read,
}

/// A storage interceptor that fails one operation on one named key for the first
/// `fail_times` matching calls, counts matching calls, then delegates normally.
struct FailStorageOpOnKey {
    op: FailingCloudOp,
    key: String,
    fail_times: usize,
    calls: Arc<AtomicUsize>,
}

impl FailStorageOpOnKey {
    fn new(op: FailingCloudOp, key: &str, fail_times: usize) -> Self {
        Self {
            op,
            key: key.to_string(),
            fail_times,
            calls: Arc::new(AtomicUsize::new(0)),
        }
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
impl StorageInterceptor for FailStorageOpOnKey {
    async fn before_provider_object_write(&self, key: &str) -> Result<(), StorageError> {
        if self.should_fail(FailingCloudOp::PutObject, key) {
            return Err(StorageError::Storage(format!(
                "injected put_object failure for {key}"
            )));
        }
        Ok(())
    }

    async fn before_provider_object_read(&self, key: &str) -> Result<(), StorageError> {
        if self.should_fail(FailingCloudOp::Read, key) {
            return Err(StorageError::Storage(format!(
                "injected read failure for {key}"
            )));
        }
        Ok(())
    }

    async fn before_provider_object_delete(&self, key: &str) -> Result<(), StorageError> {
        if self.should_fail(FailingCloudOp::Delete, key) {
            return Err(StorageError::Storage(format!(
                "injected delete failure for {key}"
            )));
        }
        Ok(())
    }

    async fn before_provider_object_exists(
        &self,
        key: &str,
    ) -> Result<ProviderObjectExistsInterception, StorageError> {
        if self.should_fail(FailingCloudOp::Exists, key) {
            return Err(StorageError::Storage(format!(
                "injected exists failure for {key}"
            )));
        }
        Ok(ProviderObjectExistsInterception::Proceed)
    }
}

// ----- the outbox Delete row becomes a tombstone and is removed -----

/// A queued blob delete drains to a signed cloud tombstone (the deletion's durable
/// record) and the outbox row is cleared — and crucially the blob is NOT deleted
/// yet (it is kept for the convergence grace).
#[tokio::test]
async fn enqueued_delete_becomes_a_tombstone_and_clears_the_outbox() {
    let db = open_outbox_db();
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "queued-delete", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deletes_before = storage.tombstone_deletions();

    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");

    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_at(&store_database, &*storage.storage(), &cipher, &kp, &clock)
        .await
        .expect("drain");
    assert_eq!(n, 1, "one tombstone written");

    // The blob is still present — the drain records the deletion, it doesn't
    // perform it.
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob is kept for the grace, not deleted on drain",
    );
    assert_eq!(
        storage.tombstone_deletions(),
        deletes_before,
        "the drain deletes no provider object",
    );

    // A signed tombstone landed at the derived key and verifies under the store.
    let tombstone_bytes = storage
        .stored_tombstone_bytes(&tombstone_key)
        .expect("tombstone object present");
    let tombstone: BlobTombstoneJson =
        serde_json::from_slice(&tombstone_bytes).expect("parse tombstone");
    assert_eq!(tombstone.stored, stored);
    assert_eq!(tombstone.deleted_at, "2024-06-10T00:00:00+00:00");
    assert_eq!(tombstone.author_pubkey, hex::encode(kp.public_key()));
    assert!(tombstone.verify("lib"), "the tombstone is validly signed");

    // The outbox row is gone.
    assert!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the drained delete row is removed",
    );
}

#[tokio::test]
async fn garbage_at_the_tombstone_key_does_not_clear_the_delete() {
    let db = open_outbox_db();
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "garbage-slot", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    storage
        .plant_tombstone_bytes(&tombstone_key, b"not a tombstone".to_vec())
        .await
        .expect("plant garbage");

    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");
    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_at(&store_database, &*storage.storage(), &cipher, &kp, &clock)
        .await
        .expect("drain");

    assert_eq!(n, 1, "the garbage object is replaced by a signed tombstone");
    let tombstone_bytes = storage
        .stored_tombstone_bytes(&tombstone_key)
        .expect("tombstone object present");
    let tombstone: BlobTombstoneJson =
        serde_json::from_slice(&tombstone_bytes).expect("parse tombstone");
    assert_eq!(tombstone.stored, stored);
    assert!(tombstone.verify("lib"));
    assert!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .is_empty(),
        "the delete row clears only after a valid tombstone is present",
    );
}

#[tokio::test]
async fn valid_existing_tombstone_is_preserved_by_delete_drain() {
    let db = open_outbox_db();
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "existing", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let original = BlobTombstoneJson::signed(
        "lib",
        stored.clone(),
        "2024-06-01T00:00:00+00:00".to_string(),
        &kp,
    );
    storage.plant_tombstone(&original).await;
    let original_bytes = storage
        .stored_tombstone_bytes(&tombstone_key)
        .expect("original tombstone");

    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");
    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_at(&store_database, &*storage.storage(), &cipher, &kp, &clock)
        .await
        .expect("drain");

    assert_eq!(n, 0, "an existing valid tombstone is not rewritten");
    assert_eq!(
        storage.stored_tombstone_bytes(&tombstone_key),
        Some(original_bytes),
        "the existing tombstone remains byte-for-byte unchanged",
    );
    assert!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .is_empty(),
        "the delete row clears because the valid tombstone is already present",
    );
}

/// An upload row reads back as an `Upload` carrying its scope; a delete row reads
/// back as a `Delete`. The operation-specific fields live in the variant, so a
/// delete has no scope to be `None` — the shared `cloud_outbox` row-shape
/// contract.
#[tokio::test]
async fn upload_carries_scope_delete_carries_no_extra_fields() {
    use coven_database::{OutboxOperation, OutboxUploadState};

    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    db.plant_blob_row_for_test("upload-row", false, b"upload body")
        .await;
    let row = db
        .row_blob_ref("note_photos", "upload-row")
        .await
        .expect("load exact upload row");
    let source_dir = tempfile::tempdir().expect("create upload source directory");
    let source_path = source_dir.path().join("upload-row");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&source_path, b"upload body")
        .await
        .expect("write upload source");
    let enqueue_row = row.clone();
    let enqueue_path = source_path.clone();
    db.test_sql(move |database| {
        database.enqueue_blob_upload(
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
    let storage = test_store().await;
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "shape-delete", b"delete")
        .await;
    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");

    let uploads = coven_database::StoreDatabase::new(&db)
        .pending_blob_uploads()
        .await
        .expect("uploads");
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

    let deletes = coven_database::StoreDatabase::new(&db)
        .pending_blob_deletes()
        .await
        .expect("deletes");
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].operation, OutboxOperation::Delete { stored });
}

/// A failed tombstone validation records durable retry state, then the
/// delete drain skips the row inside the backoff window and retries once the
/// window has elapsed.
#[tokio::test]
async fn delete_validation_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "validation-retry", b"body")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let failure = FailStorageOpOnKey::new(FailingCloudOp::Read, &tombstone_key, 1);
    let calls = failure.calls.clone();
    let intercepted = InterceptedStorage::new(storage.storage(), failure);
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let error = drain_at(&store_database, &intercepted, &cipher, &kp, &first)
        .await
        .expect_err("failed validation fails the drain");
    assert!(error.contains("Failed to validate"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let first_row = db
        .delete_outbox_attempt_for_test(1)
        .await
        .expect("query delete outbox entry")
        .expect("delete row remains");
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
    let n = drain_at(&store_database, &intercepted, &cipher, &kp, &inside)
        .await
        .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "inside the backoff window no cloud validation runs",
    );
    assert_eq!(
        db.delete_outbox_attempt_for_test(1)
            .await
            .expect("query delete outbox entry")
            .expect("delete row remains"),
        first_row,
        "the skipped row is unchanged",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_at(&store_database, &intercepted, &cipher, &kp, &after)
        .await
        .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        storage.stored_tombstone_bytes(&tombstone_key).is_some(),
        "the retried drain writes the tombstone",
    );
    assert!(
        db.delete_outbox_attempt_for_test(1)
            .await
            .expect("query delete outbox entry")
            .is_none(),
        "the successful retry clears the delete row",
    );
}

/// A failed tombstone write records the same durable retry state as an existence
/// check failure, and the backoff gate suppresses the write attempt until the
/// retry window has elapsed.
#[tokio::test]
async fn delete_write_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "write-retry", b"body")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let failure = FailStorageOpOnKey::new(FailingCloudOp::PutObject, &tombstone_key, 1);
    let calls = failure.calls.clone();
    let intercepted = InterceptedStorage::new(storage.storage(), failure);
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let error = drain_at(&store_database, &intercepted, &cipher, &kp, &first)
        .await
        .expect_err("failed write fails the drain");
    assert!(error.contains("Tombstone write failed"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let first_row = db
        .delete_outbox_attempt_for_test(1)
        .await
        .expect("query delete outbox entry")
        .expect("delete row remains");
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
    let n = drain_at(&store_database, &intercepted, &cipher, &kp, &inside)
        .await
        .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "inside the backoff window no cloud write runs",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_at(&store_database, &intercepted, &cipher, &kp, &after)
        .await
        .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        storage.stored_tombstone_bytes(&tombstone_key).is_some(),
        "the retried drain writes the tombstone",
    );
}

/// A corrupt durable retry timestamp makes the schedule unknowable. The drain
/// fails before any remote effect or journal mutation rather than treating the
/// entry as due and overwriting the evidence.
#[tokio::test]
async fn corrupt_delete_backoff_timestamp_fails_before_remote_effects() {
    tokio::task::LocalSet::new()
        .run_until(async {
            tokio::task::spawn_local(async {
                let db = open_outbox_db();
                let store_database = StoreDatabase::new(&db);
                let storage = Box::pin(test_store()).await;
                let corrupt = Box::pin(storage.create_exact_opaque_blob(
                    "delete-tests",
                    "corrupt-retry",
                    b"corrupt",
                ))
                .await;
                let healthy = Box::pin(storage.create_exact_opaque_blob(
                    "delete-tests",
                    "healthy-retry",
                    b"healthy",
                ))
                .await;
                let cipher = plaintext_cipher();
                let kp = UserKeypair::generate();

                Box::pin(db.enqueue_blob_delete_for_test(&corrupt, T0))
                    .await
                    .expect("enqueue corrupt exact blob deletion");
                Box::pin(db.enqueue_blob_delete_for_test(&healthy, T0))
                    .await
                    .expect("enqueue healthy exact blob deletion");

                // The later row carries an attempt count, so a *parseable* recent timestamp
                // would hold it inside its backoff window. Its corruption must be found
                // before the earlier healthy row produces a remote effect.
                Box::pin(db.test_sql(|database| database.corrupt_delete_outbox_attempt_time(2)))
                    .await
                    .expect("corrupt last_attempt_at");

                let clock = FixedClock(at("2024-06-01T00:00:10Z"));
                let pending_rotation = PendingRotation::none();
                let drain_storage = storage.storage();
                let drain = TombstoneDrain::new(
                    &store_database,
                    &*drain_storage,
                    &cipher,
                    &pending_rotation,
                    "lib",
                    &kp,
                    &clock,
                );
                let error = Box::pin(drain.drain())
                    .await
                    .expect_err("a corrupt retry timestamp fails the drain");
                assert!(error.contains("unparseable last_attempt_at"), "{error}");
                assert!(
                    storage
                        .stored_tombstone_bytes(&exact_tombstone_key(&corrupt))
                        .is_none(),
                    "the corrupt entry produces no remote effect",
                );
                assert!(
                    storage
                        .stored_tombstone_bytes(&exact_tombstone_key(&healthy))
                        .is_none(),
                    "the drain stops before later entries",
                );
                assert!(
                    Box::pin(db.delete_outbox_attempt_for_test(1))
                        .await
                        .expect("query earlier delete outbox entry")
                        .is_some(),
                    "the earlier healthy journal row remains unchanged",
                );
                assert!(
                    Box::pin(db.delete_outbox_attempt_for_test(2))
                        .await
                        .expect("query corrupt delete outbox entry")
                        .is_some(),
                    "the corrupt journal row remains unchanged",
                );
            })
            .await
            .expect("corrupt timestamp case task completes");
        })
        .await;
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
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "grace", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    // A GC one day later — well inside the 7-day grace — keeps the blob.
    let inside = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = collector.collect(&inside).await.expect("gc inside grace");
    assert_eq!(n, 0, "nothing reclaimed inside the grace");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob survives a GC inside the grace",
    );
    assert!(
        tombstone_exists(&storage, &tombstone_key).await,
        "the tombstone is kept inside the grace",
    );

    // A GC just past the grace reclaims the blob and the tombstone.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc past grace");
    assert_eq!(n, 1, "one blob reclaimed past the grace");
    assert!(
        !storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob is deleted past the grace",
    );
    assert!(
        !tombstone_exists(&storage, &tombstone_key).await,
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
    db.insert_local_blob_row_for_test("n1", "bloblive", "bloblive", None, b"live contents")
        .await
        .expect("insert journaled Local blob row");
    let stored = storage
        .publish_exact_remote_blob_binding(&store_dir, "n1", "bloblive", b"live contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");

    assert_eq!(n, 0, "a live blob reference prevents reclaim");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the referenced blob remains in cloud",
    );
    assert!(
        !tombstone_exists(&storage, &tombstone_key).await,
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
    let replaced = storage
        .create_exact_opaque_blob("photos", "p1cover", b"old cover")
        .await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    db.insert_local_blob_row_for_test(
        "n1",
        "ph1",
        "p2cover",
        Some("n1/cover-p2cover.jpg"),
        b"live cover",
    )
    .await
    .expect("insert journaled Local blob row");
    let live = storage
        .publish_exact_remote_blob_binding(&store_dir, "n1", "ph1", b"live cover")
        .await;
    // The replacement tombstoned the blob it replaced. A tombstone also stands for the
    // live blob's key — a stale one the GC must cancel, not act on.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    for stored in [replaced.clone(), live.clone()] {
        let tombstone = signed_store_tombstone(&storage, stored, deleted_at.to_string(), &member);
        storage.plant_tombstone(&tombstone).await;
    }
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let reclaimed = collector.collect(&past).await.expect("gc");

    assert_eq!(reclaimed, 1, "exactly the replaced blob is reclaimed");
    assert!(
        !storage
            .contains_stored_blob_object(&replaced)
            .await
            .expect("verify exact blob object"),
        "the replaced blob's object is collected — no live row names its key",
    );
    assert!(
        storage
            .contains_stored_blob_object(&live)
            .await
            .expect("verify exact blob object"),
        "the blob the row now holds is protected by the live-row check",
    );
    assert!(
        !tombstone_exists(&storage, &exact_tombstone_key(&live)).await,
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
    db.insert_local_blob_row_for_test("n1", "bloblive", "bloblive", None, b"live contents")
        .await
        .expect("insert journaled Local blob row");
    let stored = storage
        .publish_exact_remote_blob_binding(&store_dir, "n1", "bloblive", b"live contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);

    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    // GC inside the grace, with the row still reading live+remote: the fresh tombstone
    // must NOT be canceled.
    let within = FixedClock(at(deleted_at));
    let n = collector.collect(&within).await.expect("gc within grace");
    assert_eq!(n, 0, "nothing reclaimed inside the grace");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob survives inside the grace",
    );
    assert!(
        tombstone_exists(&storage, &tombstone_key).await,
        "the tombstone survives inside the grace despite the stale live row",
    );

    // The peer pulls the retraction (the row is gone); past grace the blob is reclaimed.
    db.execute_test_sql(
        "DELETE FROM note_photos WHERE id = 'bloblive'; DELETE FROM notes WHERE id = 'n1'",
    )
    .await;
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc past grace");
    assert_eq!(
        n, 1,
        "the blob is reclaimed once grace passes and the row is gone"
    );
    assert!(
        !storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob is deleted past the grace",
    );
    assert!(
        !tombstone_exists(&storage, &tombstone_key).await,
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
    db.insert_local_blob_row_for_test("n1", "orphan", "orphan", None, b"orphan contents")
        .await
        .expect("insert journaled Local blob row");
    let stored = storage
        .publish_exact_remote_blob_binding(&store_dir, "n1", "orphan", b"orphan contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);

    // Remove the bound row's parent with foreign keys disabled. The exact child
    // binding remains, but locality resolution reaches no root row.
    db.execute_test_sql(
        "PRAGMA foreign_keys=OFF; \
         DELETE FROM notes WHERE id = 'n1'; \
         PRAGMA foreign_keys=ON",
    )
    .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let error = collector
        .collect(&past)
        .await
        .expect_err("unresolved locality fails GC");

    assert!(error.contains("locality is unresolved"), "{error}");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob is untouched when locality is unresolved",
    );
    assert!(
        tombstone_exists(&storage, &tombstone_key).await,
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
    let member_device = storage
        .activate_joined_device(&db, &member_db, &member, "2024-06-01T00:00:00Z")
        .await
        .expect("activate exact member uploader");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    storage.pull_into(&db, &store_dir).await;

    // One blob under the member's own prefix, one under the founder's. No live rows
    // reference either (the DB is empty), so both are ripe for reclaim — only the
    // prefix gate decides which the member may delete.
    let mine = member_device
        .create_exact_opaque_blob("photos", "mineblob", b"mine")
        .await;
    let foreign = storage
        .create_exact_opaque_blob("photos", "foreignblob", b"foreign")
        .await;
    assert_eq!(
        StoreDatabase::new(&member_db)
            .activated_store_device_registration(mine.locator().uploader().clone())
            .await
            .expect("member uploader activation is visible to GC")
            .value()
            .author_pubkey,
        pubkey_hex(&member),
    );
    let deleted_at = "2024-06-01T00:00:00+00:00";
    for stored in [mine.clone(), foreign.clone()] {
        let tombstone = signed_store_tombstone(&storage, stored, deleted_at.to_string(), &member);
        storage.plant_tombstone(&tombstone).await;
    }
    let collector = TombstoneCollector::load(
        StoreDatabase::new(&member_db),
        &storage,
        storage.storage(),
        &cipher,
        &member,
    )
    .await
    .expect("load member tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");

    assert_eq!(n, 1, "the member reclaims exactly its own-prefix blob");
    assert!(
        !storage
            .contains_stored_blob_object(&mine)
            .await
            .expect("verify exact blob object"),
        "the member's own-prefix blob is reclaimed",
    );
    assert!(
        storage
            .contains_stored_blob_object(&foreign)
            .await
            .expect("verify exact blob object"),
        "a blob under another member's prefix is left for its owner or an owner sweep",
    );
    assert!(
        tombstone_exists(&storage, &exact_tombstone_key(&foreign)).await,
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
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let member_db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let member_device = storage
        .activate_joined_device(&db, &member_db, &member, "2024-06-01T00:00:00Z")
        .await
        .expect("activate exact member uploader");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    storage.pull_into(&db, &store_dir).await;

    let foreign = member_device
        .create_exact_opaque_blob("photos", "absentblob", b"contents")
        .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, foreign.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load Owner tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");

    assert_eq!(
        n, 1,
        "the owner reclaims the absent member's condemned blob"
    );
    assert!(
        !storage
            .contains_stored_blob_object(&foreign)
            .await
            .expect("verify exact blob object"),
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
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "cancel-mid-gc", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;

    // The cloud home removes the tombstone in GC's re-check window.
    let racing_storage = Arc::new(InterceptedStorage::new(
        storage.storage(),
        CancelTombstoneOnExists {
            tombstone_key,
            fired: AtomicBool::new(false),
        },
    ));
    let collector = TombstoneCollector::for_founder_with_storage(
        StoreDatabase::new(&db),
        &storage,
        racing_storage,
        &cipher,
    )
    .await
    .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");
    assert_eq!(n, 0, "a tombstone removed mid-GC reclaims nothing");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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

    let stored = storage
        .create_exact_opaque_blob("delete-tests", "plain-delete", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc past grace");

    assert_eq!(n, 1, "the blob is erased past the grace");
    assert!(
        !storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob is gone after a plain-delete reclaim",
    );
    assert!(
        !tombstone_exists(&storage, &tombstone_key).await,
        "the tombstone is removed after reclaiming its blob",
    );
}

/// A grace the host configures (here one hour) is what the GC ages against, not
/// the seven-day default: within the hour the blob survives; past it the blob is
/// erased. The reader evaluates whatever grace it is handed.
#[tokio::test]
async fn a_configured_one_hour_grace_is_honored() {
    let grace = chrono::Duration::hours(1);
    let db = open_test_db_with_tombstone_grace(grace);
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();

    let stored = storage
        .create_exact_opaque_blob("delete-tests", "configured-grace", b"contents")
        .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    // Half an hour in — inside the configured hour — the blob survives.
    let within = FixedClock(at("2024-06-01T00:30:00Z"));
    let n = collector
        .collect(&within)
        .await
        .expect("gc within the configured hour");
    assert_eq!(n, 0, "nothing reclaimed inside the configured hour");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
        "the blob survives inside the configured hour (well before the 7-day default)",
    );

    // Just past the hour — past the configured grace though far inside the default —
    // the blob is erased.
    let past = FixedClock(at("2024-06-01T01:00:01Z"));
    let n = collector
        .collect(&past)
        .await
        .expect("gc past the configured hour");
    assert_eq!(n, 1, "the blob is erased once the configured hour passes");
    assert!(
        !storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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

    let stored = storage
        .create_exact_opaque_blob("delete-tests", "non-member", b"contents")
        .await;
    // Validly signed by the outsider (the signature itself verifies), but the
    // outsider is not a member — long past the grace, so only authorization stands
    // between this and a deletion.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &outsider);
    assert!(
        tombstone.verify(&storage.root.store_root_id.to_string()),
        "the forged tombstone is self-consistently signed (only authorization rejects it)",
    );
    storage.plant_tombstone(&tombstone).await;
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");
    assert_eq!(n, 0, "a non-member tombstone reclaims nothing");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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
    let storage = TestStore::create(
        &attacker_db,
        "test-store",
        attacker.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create attacker-founded exact Store");
    // The victim blob, and a tombstone the attacker signs as their forged founder,
    // backdated well past the grace so only authorization stands between it and the
    // deletion.
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "refounded", b"contents")
        .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &attacker);
    assert!(
        tombstone.verify(&storage.root.store_root_id.to_string()),
        "the forged tombstone is self-consistently signed (only owner-anchored \
         authorization rejects it)",
    );
    storage.plant_tombstone(&tombstone).await;

    // Opening this exact Store under the established owner refuses the forged root
    // before a cycle can load membership or run GC.
    let joining_db = open_test_db();
    let (_store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let result = storage
        .open_store_with_identity(&joining_db, store_dir, &real_owner)
        .await;
    assert!(
        result.is_err(),
        "loading a chain refounded under a non-owner key refuses the cycle before \
         any tombstone is judged",
    );
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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
    let storage = TestStore::create(
        &founder_db,
        "test-store",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store before wiping its membership head");
    let founder_graph = coven_database::StoreDatabase::new(&founder_db)
        .local_store_founder_graph()
        .await
        .expect("load exact founder graph")
        .expect("created Store has a founder graph");
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "wiped", b"contents")
        .await;
    let coven_database::DurableFounderMembership { head_ref, .. } = founder_graph.membership;
    storage
        .delete_membership_head_for_test(&head_ref)
        .await
        .expect("wipe exact founder membership head");

    // A blob and a validly self-signed tombstone by some attacker, backdated past
    // the grace.
    let attacker = UserKeypair::generate();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &attacker);
    storage.plant_tombstone(&tombstone).await;

    let joining_db = open_test_db();
    let (_store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let result = storage
        .open_store_with_identity(&joining_db, store_dir, &real_owner)
        .await;
    assert!(
        result.is_err(),
        "an empty (wiped) chain under a pinned owner refuses the cycle at load, \
         before any tombstone is judged",
    );
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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

    let stored = storage
        .create_exact_opaque_blob("delete-tests", "bad-signature", b"contents")
        .await;
    let other = storage
        .create_exact_opaque_blob("delete-tests", "signed-other", b"other")
        .await;
    // A member signs a tombstone for another exact object, then its stored
    // reference is replaced. The signature no longer covers the object in its slot.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let mut tombstone = signed_store_tombstone(&storage, other, deleted_at.to_string(), &member);
    tombstone.stored = stored.clone();
    assert!(
        !tombstone.verify(&storage.root.store_root_id.to_string()),
        "the tampered tombstone does not verify",
    );
    storage.plant_tombstone(&tombstone).await;

    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");
    assert_eq!(n, 0, "a tombstone with a bad signature reclaims nothing");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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

    let stored = storage
        .create_exact_opaque_blob("delete-tests", "foreign-store", b"contents")
        .await;
    // Signed for "other-lib" by a real member of this Store; the signature
    // fails when GC verifies it against the Store's actual identity.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        BlobTombstoneJson::signed("other-lib", stored.clone(), deleted_at.to_string(), &member);
    assert!(
        tombstone.verify("other-lib") && !tombstone.verify(&storage.root.store_root_id.to_string()),
        "the tombstone verifies only under the store it was signed for",
    );
    storage.plant_tombstone(&tombstone).await;

    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = collector.collect(&past).await.expect("gc");
    assert_eq!(n, 0, "a foreign-store tombstone reclaims nothing");
    assert!(
        storage
            .contains_stored_blob_object(&stored)
            .await
            .expect("verify exact blob object"),
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
    let storage = test_store().await;
    let deleted = storage
        .create_exact_opaque_blob("photos", "same-id", b"old")
        .await;

    // Delete then upload: both exact intents remain.
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let sources = tempfile::tempdir().expect("upload sources");
    let owners = TestOwnerGraph::new(
        StoreDatabase::new(&db),
        StoreDir::new(sources.path().join("store")),
    );
    db.enqueue_blob_delete_for_test(&deleted, T0)
        .await
        .expect("enqueue exact blob deletion");
    db.plant_blob_row_for_test("same-id", false, b"replacement")
        .await;
    owners
        .stage_pending_upload_for_test(sources.path(), "same-id", b"replacement", T0)
        .await;
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_uploads()
            .await
            .unwrap()
            .len(),
        1,
        "the exact row-version upload remains",
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .len(),
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
    let owners = TestOwnerGraph::new(
        StoreDatabase::new(&db),
        StoreDir::new(sources.path().join("store")),
    );
    db.plant_blob_row_for_test("same-id", false, b"replacement")
        .await;
    owners
        .stage_pending_upload_for_test(sources.path(), "same-id", b"replacement", T0)
        .await;
    db.enqueue_blob_delete_for_test(&deleted, T0)
        .await
        .expect("enqueue exact blob deletion");
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_uploads()
            .await
            .unwrap()
            .len(),
        1,
        "the exact row-version upload remains",
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .len(),
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
    let owners = TestOwnerGraph::new(
        StoreDatabase::new(&db),
        StoreDir::new(sources.path().join("store")),
    );
    db.plant_blob_row_for_test("other-id", false, b"other")
        .await;
    owners
        .stage_pending_upload_for_test(sources.path(), "other-id", b"other", T0)
        .await;
    db.enqueue_blob_delete_for_test(&deleted, T0)
        .await
        .expect("enqueue exact blob deletion");
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_uploads()
            .await
            .unwrap()
            .len(),
        1,
        "the unrelated exact upload remains",
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .len(),
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

    let old = storage
        .create_exact_opaque_blob("photos", "blob-key", b"old contents")
        .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = signed_store_tombstone(&storage, old.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;

    db.insert_local_blob_row_for_test(
        "note-blob-key",
        "blob-key",
        "blob-key",
        None,
        b"fresh contents",
    )
    .await
    .expect("insert journaled Local blob row");
    let store_dir = StoreDir::new(tmp.path());
    let replacement = storage
        .publish_exact_remote_blob_binding(
            &store_dir,
            "note-blob-key",
            "blob-key",
            b"fresh contents",
        )
        .await;
    assert_ne!(old.object(), replacement.object());
    assert!(storage
        .contains_stored_blob_object(&old)
        .await
        .expect("verify exact blob object"));
    assert!(storage
        .contains_stored_blob_object(&replacement)
        .await
        .expect("verify exact blob object"));
    assert!(tombstone_exists(&storage, &exact_tombstone_key(&old)).await);

    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let gc = collector.collect(&past).await.expect("gc");
    assert_eq!(gc, 1, "the old exact object is reclaimed");
    assert!(!storage
        .contains_stored_blob_object(&old)
        .await
        .expect("verify exact blob object"));
    assert!(storage
        .contains_stored_blob_object(&replacement)
        .await
        .expect("verify replacement exact blob object"));
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
    let store_database = StoreDatabase::new(&db);
    let storage = test_store().await;
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    let stored = storage
        .create_exact_opaque_blob("delete-tests", "redrain", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&stored);

    // First drain at T0: writes the tombstone, removes the row.
    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("enqueue exact blob deletion");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let n = drain_at(&store_database, &*storage.storage(), &cipher, &kp, &first)
        .await
        .expect("first drain");
    assert_eq!(n, 1, "the first drain writes the tombstone");
    let original_deleted_at = {
        let bytes = storage
            .stored_tombstone_bytes(&tombstone_key)
            .expect("tombstone");
        let t: BlobTombstoneJson = serde_json::from_slice(&bytes).unwrap();
        t.deleted_at
    };

    // The same deletion is queued again (its row re-appears: the prior removal
    // failed, or the host re-enqueued it) and drained a day later. The tombstone
    // already exists, so this drain must not rewrite it.
    db.enqueue_blob_delete_for_test(&stored, "2024-06-02T00:00:00Z")
        .await
        .expect("reenqueue exact blob deletion");
    let second = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = drain_at(&store_database, &*storage.storage(), &cipher, &kp, &second)
        .await
        .expect("second drain");
    assert_eq!(n, 0, "the re-drain writes no new tombstone");

    // Exactly one tombstone, still carrying the first drain's deleted_at.
    let tombstones = storage
        .storage()
        .list_provider_objects("blob_tombstones/")
        .await
        .unwrap();
    assert_eq!(tombstones.len(), 1, "only one tombstone exists for the key");
    let bytes = storage
        .stored_tombstone_bytes(&tombstone_key)
        .expect("tombstone");
    let tombstone: BlobTombstoneJson = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        tombstone.deleted_at, original_deleted_at,
        "the re-drain preserves the original deleted_at — the grace is not reset",
    );

    // The re-enqueued row is removed too (the drain always removes the row once the
    // tombstone is present), so the queue converges.
    assert!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .is_empty(),
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
    let old = storage
        .create_exact_opaque_blob("photos", "blob", b"same bytes")
        .await;
    let replacement = storage
        .create_exact_opaque_blob("photos", "blob", b"same bytes")
        .await;
    assert_ne!(
        old.object(),
        replacement.object(),
        "a replacement owns a distinct exact provider object",
    );
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = signed_store_tombstone(&storage, old.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;

    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let reclaimed = collector
        .collect(&past)
        .await
        .expect("reclaim the exact replaced object");
    assert_eq!(reclaimed, 1);
    assert!(!storage
        .contains_stored_blob_object(&old)
        .await
        .expect("verify exact blob object"));
    assert!(storage
        .contains_stored_blob_object(&replacement)
        .await
        .expect("verify replacement exact blob object"));
}

/// A failed exact-object delete leaves the signed tombstone in place. A later GC
/// retries the same exact reference and removes both objects after the provider
/// accepts the delete.
#[tokio::test]
async fn exact_blob_delete_failure_leaves_tombstone_for_retry() {
    let db = open_test_db();
    let (storage, _founder, member) = storage_with_chain(&db).await;
    let cipher = plaintext_cipher();
    let stored = storage
        .create_exact_opaque_blob("photos", "retry", b"retry bytes")
        .await;
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone =
        signed_store_tombstone(&storage, stored.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;
    storage.fail_exact_delete_on_call(1);
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let past = FixedClock(at(&past_grace_instant(deleted_at)));

    let error = collector
        .collect(&past)
        .await
        .expect_err("exact delete failure fails GC");
    assert!(error.contains("Failed to delete blob"), "{error}");
    assert!(storage
        .contains_stored_blob_object(&stored)
        .await
        .expect("verify failed exact delete blob"));
    assert!(tombstone_exists(&storage, &exact_tombstone_key(&stored)).await);

    let second = collector.collect(&past).await.expect("retry GC");
    assert_eq!(second, 1);
    assert!(!storage
        .contains_stored_blob_object(&stored)
        .await
        .expect("verify exact blob object"));
    assert!(!tombstone_exists(&storage, &exact_tombstone_key(&stored)).await);
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
    let old = storage
        .create_exact_opaque_blob("photos", "blob-key", b"contents")
        .await;
    let tombstone_key = exact_tombstone_key(&old);
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = signed_store_tombstone(&storage, old.clone(), deleted_at.to_string(), &member);
    storage.plant_tombstone(&tombstone).await;

    // First GC past the grace: the blob delete succeeds, but the tombstone delete
    // fails (injected). The blob is reclaimed; the tombstone is left behind.
    let failing = FailStorageOpOnKey::new(FailingCloudOp::Delete, &tombstone_key, 1);
    let failing_storage = Arc::new(InterceptedStorage::new(storage.storage(), failing));
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let failing_collector = TombstoneCollector::for_founder_with_storage(
        StoreDatabase::new(&db),
        &storage,
        failing_storage,
        &cipher,
    )
    .await
    .expect("load failing tombstone collector");
    let error = failing_collector
        .collect(&past)
        .await
        .expect_err("tombstone delete failure fails GC");
    assert!(error.contains("Failed to delete tombstone"), "{error}");
    assert!(
        !storage
            .contains_stored_blob_object(&old)
            .await
            .expect("verify exact blob object"),
        "the blob is deleted",
    );
    assert!(
        tombstone_exists(&storage, &tombstone_key).await,
        "the tombstone is left behind because its delete failed",
    );

    // Upload a replacement while the stale exact tombstone remains.
    let tmp = tempfile::tempdir().unwrap();
    db.insert_local_blob_row_for_test(
        "note-blob-key",
        "blob-key",
        "blob-key",
        None,
        b"re-uploaded contents",
    )
    .await
    .expect("insert journaled Local blob row");
    let store_dir = StoreDir::new(tmp.path());
    let replacement = storage
        .publish_exact_remote_blob_binding(
            &store_dir,
            "note-blob-key",
            "blob-key",
            b"re-uploaded contents",
        )
        .await;
    assert_ne!(old.object(), replacement.object());
    assert!(
        tombstone_exists(&storage, &tombstone_key).await,
        "the replacement does not cancel a tombstone for another exact object",
    );

    // The next GC sees the old object already absent, removes its tombstone without
    // counting another reclaim, and leaves the replacement object intact.
    let collector = TombstoneCollector::for_founder(StoreDatabase::new(&db), &storage, &cipher)
        .await
        .expect("load tombstone collector");
    let n = collector.collect(&past).await.expect("gc after re-upload");
    assert_eq!(n, 0, "the absent old object is not counted twice");
    assert!(
        !tombstone_exists(&storage, &tombstone_key).await,
        "the leftover tombstone is cleaned up",
    );
    assert!(
        storage
            .contains_stored_blob_object(&replacement)
            .await
            .expect("verify exact blob object"),
        "the re-uploaded blob survives the leftover-tombstone window",
    );
}
