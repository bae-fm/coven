//! Blob-before-row ordering is enforced per row by the gate column: a blob-bearing
//! row's gate column stays off until its blobs upload, then coven flips it on (the
//! manage completion in the upload drain), so the changeset gate — and the snapshot,
//! which runs the same gate — only ever carry rows whose blobs are in the cloud. The
//! sync cycle does not hold the whole changeset back on a global "any upload
//! pending" flag.
//!
//! These tests pin that contract: a pending upload does not hold back an
//! already-shareable (gated-true) changeset or snapshot, and a gated-false row is
//! withheld until its gate flips. The completion flip + its mid-batch publish
//! (`resume_drain_promptly`) are covered in `blob::transition_tests`.

#[path = "cycle_tests/join_authority_tests.rs"]
mod join_authority_tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::sync::cycle;
use crate::sync::test_helpers::*;
use coven_database::Database;
use coven_database::StoreDatabase;
use coven_foundation::clock::{FixedClock, SystemClock};
use coven_foundation::store_dir::StoreDir;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::store_commit::SnapshotMeta;
use coven_protocol::synced_schema::{BlobDecl, SyncedTable};
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::CloudSyncObjectStorage;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

#[path = "cycle_tests/provider_access_publication.rs"]
mod provider_access_publication;

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`coven_database::Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

fn cycle_cloud_storage(
    home: Arc<dyn coven_storage::ExactCloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> CloudSyncConnection {
    CloudSyncConnection::new(home, cipher, blob_paths, store_id, keypair)
}

async fn cycle_test_store(
    db: &Database,
    db_store_dir: StoreDir,
    signer: &UserKeypair,
    home: Arc<coven_storage::InMemoryCloudHome>,
) -> std::sync::Arc<TestStore> {
    let (store, _connection) = cycle_test_store_fixture(db, db_store_dir, signer, home).await;
    store
}

async fn cycle_test_store_fixture(
    db: &Database,
    db_store_dir: StoreDir,
    signer: &UserKeypair,
    home: Arc<coven_storage::InMemoryCloudHome>,
) -> TestStoreParts {
    TestStore::create_with_connection(db, db_store_dir.clone(), "test-lib", signer.clone(), home)
        .await
        .expect("create exact cycle test Store fixture")
}

/// A fresh owner Store plus the second identity its device-join cases admit.
struct OwnerAndMember {
    owner: UserKeypair,
    owner_db: Database,
    owner_db_store_dir: StoreDir,
    storage: Arc<TestStore>,
    cloud_storage: Arc<CloudSyncConnection>,
    member: UserKeypair,
}

async fn owner_and_member() -> OwnerAndMember {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let fixture = cycle_test_store_fixture(
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (storage, cloud_storage) = fixture;
    OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member: UserKeypair::generate(),
    }
}

fn cross_principal_test_home() -> Arc<InMemoryCloudHome> {
    use coven_protocol::objects::{
        ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, StoreProviderBinding,
    };

    crate::sync::test_helpers::test_cloud_home_with_binding(ResolvedProviderBinding {
        store: StoreProviderBinding::Dropbox {
            namespace_id: "shared-namespace".to_string(),
        },
        device: ProviderDeviceBinding {
            principal: ProviderPrincipalId::Dropbox {
                account_id: "administrator-account".to_string(),
            },
        },
    })
}

async fn cross_principal_owner_and_member() -> OwnerAndMember {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let (storage, cloud_storage) = cycle_test_store_fixture(
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        cross_principal_test_home(),
    )
    .await;
    OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member: UserKeypair::generate(),
    }
}

/// Admits `member` into the owner's Store as an ordinary member.
async fn admit_test_member(
    storage: &TestStore,
    owner_db: &Database,
    owner_db_store_dir: StoreDir,
    owner: &UserKeypair,
    member: &UserKeypair,
    encryption: &EncryptionService,
) {
    storage
        .admit_member(
            owner_db,
            owner_db_store_dir.clone(),
            owner,
            &pubkey_hex(member),
            None,
            coven_protocol::membership::MemberRole::Member,
            encryption,
            "Test Store",
        )
        .await
        .expect("admit exact Member identity");
}

/// A `note_photos` schema whose rows carry a blob at `fill`, plus the Store its
/// owner publishes through.
async fn blob_cycle_store(
    keypair: &UserKeypair,
    fill: CacheFill,
) -> (Database, StoreDir, Arc<TestStore>, Arc<CloudSyncConnection>) {
    let db_store_dir = test_store_dir();
    let db = open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("photos", Provenance::HostProvided, fill),
    );
    let fixture = cycle_test_store_fixture(
        &db,
        db_store_dir.clone(),
        keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (storage, cloud_storage) = fixture;
    (db, db_store_dir, storage, cloud_storage)
}

async fn run_cycle_in_task(
    storage: Arc<CycleStorageInterceptor>,
    device: TestDevice,
) -> Result<(), cycle::SyncCycleFailure> {
    tokio::spawn(async move { storage.run_sync_cycle(&device).await.map(|_| ()) })
        .await
        .expect("cycle task completes")
}

#[tokio::test]
async fn tombstone_provider_failure_fails_cycle_and_preserves_intent() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let keypair = UserKeypair::generate();
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    let stored = storage
        .create_exact_opaque_blob("photos", "maintenance", b"maintenance")
        .await;
    db.enqueue_blob_delete_for_test(&stored, T0)
        .await
        .expect("queue exact maintenance tombstone");
    storage.arm_provider_write_failures();
    let result = device.run_cycle(None).await;
    let error = result.expect_err("tombstone publication failure fails the cycle");
    assert!(error.contains("drain queued blob tombstones"), "{error}");
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .pending_blob_deletes()
            .await
            .unwrap()
            .len(),
        1,
        "failed maintenance remains queued"
    );
}

trait CycleTestDatabaseOps {
    async fn local_store_stream_id(&self) -> String;
    async fn latest_store_snapshot_meta(&self) -> Option<SnapshotMeta>;
    async fn stored_blob_for_row(
        &self,
        table: &str,
        row_id: &str,
    ) -> Option<coven_protocol::blob::locator::StoredBlobRef>;
    async fn make_remote_intent_present(&self, root_table: &str, root_id: &str) -> bool;
    async fn pending_write_count(&self) -> i64;
}

impl CycleTestDatabaseOps for Database {
    async fn local_store_stream_id(&self) -> String {
        let local_device = self
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device")
            .expect("local Store device exists");
        let registration = coven_database::StoreDatabase::new(self)
            .activated_store_device_registration_records()
            .await
            .expect("read activated Store registrations")
            .into_iter()
            .find(|registration| registration.value().device_id.to_string() == local_device)
            .expect("local Store registration is active");
        coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            registration.value().store_root.store_root_hash,
            registration.reference(),
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
        .to_string()
    }

    async fn latest_store_snapshot_meta(&self) -> Option<SnapshotMeta> {
        store_database(self)
            .latest_local_store_snapshot()
            .await
            .expect("read latest exact Store snapshot")
            .map(|snapshot| snapshot.meta)
    }

    async fn stored_blob_for_row(
        &self,
        table: &str,
        row_id: &str,
    ) -> Option<coven_protocol::blob::locator::StoredBlobRef> {
        self.row_blob_ref(table, row_id)
            .await
            .expect("resolve exact blob row")
            .stored()
            .cloned()
    }

    async fn make_remote_intent_present(&self, root_table: &str, root_id: &str) -> bool {
        self.make_remote_intent_exists_for_test(root_table, root_id)
            .await
            .expect("make_remote intent lookup")
    }

    async fn pending_write_count(&self) -> i64 {
        i64::try_from(
            StoreDatabase::new(self)
                .pending_writes()
                .await
                .expect("pending writes")
                .len(),
        )
        .expect("pending write count fits SQLite integer")
    }
}

trait CycleTestStoreOps {
    /// Whether the Store package one exact commit published is still readable.
    ///
    /// Takes the commit reference rather than a coordinate because a coordinate
    /// stops naming a commit once a snapshot covers it: the device retires the
    /// per-position row when its replay baseline advances, and a lookup by
    /// coordinate would then report "no package" about a package that is
    /// sitting there.
    async fn store_package_exists(
        &self,
        db: &Database,
        db_store_dir: StoreDir,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> bool;
    /// Whether this device published a Store package at `sequence` on its own
    /// stream, as far as its own materialized rows record.
    ///
    /// Answers `false` for a position the device holds no row for, which is
    /// what the callers below are asserting: a write that failed or was blocked
    /// never reached the position at all.
    async fn local_store_package_exists(
        &self,
        db: &Database,
        db_store_dir: StoreDir,
        sequence: u64,
    ) -> bool;
    async fn stored_blob_exists(&self, db: &Database, table: &str, row_id: &str) -> bool;
    async fn retain_store_packages_for_assertion(&self, db: &Database, db_store_dir: StoreDir);
    async fn assert_latest_ack_timestamp_is_rfc3339(&self, db: &Database, db_store_dir: StoreDir);
}

/// Whether this device's materialized history reaches `sequence` on `stream_id`.
///
/// Deliberately not `exact_materialized_ref`. A cycle that advances its replay
/// baseline retires the per-position rows the snapshot it adopted restates, so
/// a commit at or under the new coverage stops having a row of its own — the
/// position survives in the frontier, which reads the coverage beside the rows
/// and takes the later of the two. Asking for the row would answer "not
/// materialized" about history the device demonstrably holds.
async fn materialized_history_reaches(db: &Database, stream_id: &str, sequence: u64) -> bool {
    coven_database::StoreDatabase::new(db)
        .materialized_frontier()
        .await
        .expect("read materialized Store frontier")
        .get(stream_id)
        .is_some_and(|reference| reference.coord.sequence() >= sequence)
}

impl CycleTestStoreOps for TestStore {
    async fn store_package_exists(
        &self,
        db: &Database,
        db_store_dir: StoreDir,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> bool {
        let device = self
            .bind_founder_device(db, db_store_dir.clone())
            .await
            .expect("bind Store package test device");
        match device.load_store_package_for_test(reference).await {
            Ok(package) => package.is_some(),
            Err(crate::sync::store::StoreError::Object(
                coven_protocol::objects::StoreObjectError::Storage(
                    coven_protocol::objects::StorageError::NotFound(_),
                ),
            )) => false,
            Err(error) => panic!("load Store package: {error}"),
        }
    }

    async fn local_store_package_exists(
        &self,
        db: &Database,
        db_store_dir: StoreDir,
        sequence: u64,
    ) -> bool {
        let stream_id = db.local_store_stream_id().await;
        let Some(reference) = coven_database::StoreDatabase::new(db)
            .exact_materialized_ref(&stream_id, sequence)
            .await
            .expect("read exact materialized Store position")
        else {
            return false;
        };
        self.store_package_exists(db, db_store_dir, &reference)
            .await
    }

    async fn stored_blob_exists(&self, db: &Database, table: &str, row_id: &str) -> bool {
        let Some(stored) = db.stored_blob_for_row(table, row_id).await else {
            return false;
        };
        self.contains_stored_blob_object(&stored)
            .await
            .expect("verify exact stored blob object")
    }

    async fn retain_store_packages_for_assertion(&self, db: &Database, db_store_dir: StoreDir) {
        let device = self
            .open_into(db, db_store_dir.clone())
            .await
            .expect("open exact Store before seeding snapshot");
        publish_current_snapshot(&device, T0).await;
    }

    /// The acknowledgement the cycle writes records its completion time as an RFC
    /// 3339 wall-clock string, never the HLC string used to order row writes.
    async fn assert_latest_ack_timestamp_is_rfc3339(&self, db: &Database, db_store_dir: StoreDir) {
        let published = store_database(db)
            .latest_local_store_ack()
            .await
            .expect("read latest exact Store acknowledgement")
            .expect("the cycle published an acknowledgement");
        let device = self
            .bind_founder_device(db, db_store_dir.clone())
            .await
            .expect("bind acknowledgement inspection Store");
        let local_device = db
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device")
            .expect("local Store device exists");
        let registration = coven_database::StoreDatabase::new(db)
            .activated_store_device_registration_records()
            .await
            .expect("read activated Store registrations")
            .into_iter()
            .find(|registration| registration.value().device_id.to_string() == local_device)
            .expect("local Store registration is active");
        let acknowledgement = device
            .load_store_ack_for_test(&published.reference, registration.value())
            .await
            .expect("load exact Store acknowledgement");
        assert!(
            chrono::DateTime::parse_from_rfc3339(&acknowledgement.last_sync).is_ok(),
            "acknowledgement completion time must be RFC 3339, got {:?}",
            acknowledgement.last_sync,
        );
    }
}

async fn publish_current_snapshot(device: &TestDevice, created_at: &str) -> SnapshotMeta {
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize snapshot fixture writer");
    let mut snapshots = writer.snapshots();
    let encryption = EncryptionService::from_key([42; 32]);
    let cut = snapshots
        .capture_snapshot_cut(Some(&encryption))
        .await
        .expect("capture the accepted Store snapshot");
    snapshots
        .push_snapshot_cut(cut, created_at.to_string())
        .await
        .expect("publish exact Store snapshot fixture")
}

fn fail_exact_create_on(storage: &TestStore, call: usize) {
    storage.fail_exact_create_before_call(call);
}

fn exercise_pre_attempt_abandonment<'a>(
    owner_db: &'a coven_database::StoreDatabase,
    owner_db_store_dir: &'a StoreDir,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let owner_device = storage
            .bind_store_device(owner_db, owner_db_store_dir.clone(), owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin exact device join");
        let pending_join = storage
            .open_pending_device_join(&pending, member, offer.clone())
            .await
            .expect("bind pending Store join");
        let _request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare exact provider access request");
        let abandonment = owner_device
            .abandon_device_join(offer.clone())
            .await
            .expect("abandon device join before attempt activation");
        let retried = owner_device
            .abandon_device_join(offer.clone())
            .await
            .expect("retry device join abandonment");
        assert_eq!(retried, abandonment);

        let mut observation = storage
            .pending_device_join_observation(&pending, &offer)
            .await
            .expect("open pending Store join observation");
        let observed = observation
            .observe_abandonment(abandonment.clone())
            .await
            .expect("observe exact abandonment");
        let observed_retry = observation
            .observe_abandonment(abandonment.clone())
            .await
            .expect("retry exact abandonment observation");
        assert_eq!(observed_retry, observed);
        assert!(matches!(
            owner_db
                .device_join_status(abandonment.abandonment.attempt_id, DeviceJoinRole::Owner)
            .await
            .expect("load owner join status"),
            Some(DeviceJoinStatus::Abandoned { abandonment: durable }) if durable == abandonment
        ));
        // The joining device keeps no abandoned state: accepting the
        // abandonment is its terminal step and takes the row with it, so
        // absence is what says the attempt is over. The retry above went
        // through that absence, which is why it had to answer the same.
        assert_eq!(
            pending
                .status(abandonment.abandonment.attempt_id)
                .expect("load joiner join status"),
            None,
            "the joining device kept a journal row for an abandonment it accepted",
        );
    })
}

#[derive(Clone, Copy)]
enum ExactCreateInterruption {
    BeforeVisibility,
    AfterVisibility,
}

fn exercise_provider_access_grant_create_interruption<'a>(
    owner_db: &'a coven_database::StoreDatabase,
    owner_db_store_dir: &'a StoreDir,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    interruption: ExactCreateInterruption,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let owner_device = storage
            .bind_store_device(owner_db, owner_db_store_dir.clone(), owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin exact device join");
        let attempt_id = offer.attempt_id;
        let peer = storage
            .cross_principal_device_for_test(member, "joining-account")
            .await
            .expect("bind joining provider principal");
        let pending_join = peer
            .open_pending_device_join(&pending, member, offer)
            .await
            .expect("bind pending Store join");
        let request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare exact provider access request");
        match interruption {
            ExactCreateInterruption::BeforeVisibility => storage.fail_exact_create_before_call(1),
            ExactCreateInterruption::AfterVisibility => storage.fail_exact_create_after_call(1),
        }
        let first = peer
            .authorize_device_provider_access(&owner_device, request.clone())
            .await;
        let approval = match interruption {
            ExactCreateInterruption::BeforeVisibility => {
                assert!(
                    first.is_err(),
                    "the injected create fails before visibility"
                );
                assert!(matches!(
                    owner_db
                        .device_join_status(attempt_id, DeviceJoinRole::Owner)
                        .await
                        .expect("load provider create status"),
                    Some(DeviceJoinStatus::StorePublicationPending {
                        operation: coven_protocol::store_commit::device_join_journal::OwnerJoinPublication::ProviderAccessGrant { .. },
                    })
                ));
                peer.authorize_device_provider_access(&owner_device, request)
                    .await
                    .expect("resume provider access grant creation")
            }
            ExactCreateInterruption::AfterVisibility => {
                first.expect("lost create response settles through provider verification")
            }
        };
        let retry = peer
            .authorize_device_provider_access(&owner_device, (*approval.request).clone())
            .await
            .expect("retry completed provider access authorization");
        assert_eq!(retry, approval);
        assert!(matches!(
            owner_db
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load completed provider access status"),
            Some(DeviceJoinStatus::AwaitingRegistrationRequest { .. })
        ));
    })
}

// The drain's break-to-publish is now driven by a manage *completion* (coven flips
// the gate the moment the last blob lands), not by an observer signal. It is covered
// end-to-end in `blob::transition_tests` — `resume_drain_promptly` after a manage
// completes, with another root's blob left queued.

// ---- Host writes journal; applies never do ----

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;

use coven_protocol::objects::StorageError;

/// A [`CloudSyncObjectStorage`] that injects a host write at a cycle `await` point — the
/// moment the cycle fetches an incoming changeset to apply — by running a host
/// INSERT through the same `Database` the cycle holds, once, before delegating
/// the immutable package read to the inner mock.
///
/// This models the real hazard in issue #92: a host edit committed while the
/// cycle is in its network phase. The write goes through the actor's one
/// connection (the only door) at an `await` the cycle is parked on, and the host
/// write path appends it to the durable pending-changeset journal for the next
/// cycle.
struct CycleStorageInterceptor {
    inner: Arc<TestStore>,
    interceptor: Arc<CycleStorageInterception>,
}

enum CycleStorageInterception {
    PassThrough {
        protocol_read_calls: AtomicUsize,
    },
    RejectAckCreate {
        protocol_read_calls: AtomicUsize,
    },
    InjectHostWrite {
        db: Database,
        write_sql: String,
        fired: AtomicBool,
        protocol_read_calls: AtomicUsize,
    },
    RejectBlobCreate {
        reject_create_call: usize,
        create_calls: AtomicUsize,
        attempted: std::sync::Mutex<Vec<coven_protocol::blob::locator::StoredBlobRef>>,
        protocol_read_calls: AtomicUsize,
    },
}

impl CycleStorageInterceptor {
    fn pass_through(inner: Arc<TestStore>) -> Self {
        Self::new(
            inner,
            CycleStorageInterception::PassThrough {
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn reject_ack_create(inner: Arc<TestStore>) -> Self {
        Self::new(
            inner,
            CycleStorageInterception::RejectAckCreate {
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn inject_host_write(inner: std::sync::Arc<TestStore>, db: Database, write_sql: &str) -> Self {
        Self::new(
            inner,
            CycleStorageInterception::InjectHostWrite {
                db,
                write_sql: write_sql.to_string(),
                fired: AtomicBool::new(false),
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn reject_blob_create(inner: Arc<TestStore>) -> Self {
        Self::reject_blob_create_on(inner, 1)
    }

    fn reject_blob_create_on(inner: Arc<TestStore>, reject_call: usize) -> Self {
        assert!(reject_call > 0, "blob create call numbers are 1-based");
        Self::new(
            inner,
            CycleStorageInterception::RejectBlobCreate {
                reject_create_call: reject_call,
                create_calls: AtomicUsize::new(0),
                attempted: std::sync::Mutex::new(Vec::new()),
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn new(inner: Arc<TestStore>, interceptor: CycleStorageInterception) -> Self {
        Self {
            inner,
            interceptor: Arc::new(interceptor),
        }
    }

    fn rejected_blobs(&self) -> Vec<coven_protocol::blob::locator::StoredBlobRef> {
        self.interceptor.rejected_blobs()
    }

    async fn activate_joined_device(
        &self,
        observer_db: &Database,
        observer_db_store_dir: coven_foundation::store_dir::StoreDir,
        joining_db: &Database,
        joining_db_store_dir: coven_foundation::store_dir::StoreDir,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<crate::sync::test_helpers::TestDevice, crate::sync::test_helpers::TestError> {
        self.inner
            .activate_joined_device(
                observer_db,
                observer_db_store_dir.clone(),
                joining_db,
                joining_db_store_dir.clone(),
                joining_identity,
                published_at,
            )
            .await
    }

    async fn run_sync_cycle(
        &self,
        device: &TestDevice,
    ) -> Result<cycle::SyncCycleResult, cycle::SyncCycleFailure> {
        device
            .run_cycle_with_interceptor(&SystemClock, None, None, self.interceptor.clone())
            .await
    }
}

impl CycleStorageInterception {
    fn protocol_read_calls(&self) -> &AtomicUsize {
        match self {
            Self::PassThrough {
                protocol_read_calls,
            }
            | Self::RejectAckCreate {
                protocol_read_calls,
            }
            | Self::InjectHostWrite {
                protocol_read_calls,
                ..
            }
            | Self::RejectBlobCreate {
                protocol_read_calls,
                ..
            } => protocol_read_calls,
        }
    }

    fn rejected_blobs(&self) -> Vec<coven_protocol::blob::locator::StoredBlobRef> {
        let Self::RejectBlobCreate { attempted, .. } = self else {
            panic!("storage interception does not reject blob creates");
        };
        attempted
            .lock()
            .expect("attempted blob record lock")
            .clone()
    }
}

#[async_trait]
impl crate::sync::test_helpers::StorageInterceptor for CycleStorageInterception {
    async fn before_protocol_create(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), StorageError> {
        if matches!(self, Self::RejectAckCreate { .. })
            && prepared
                .reference()
                .slot()
                .logical_key()
                .starts_with("store-v1/acks/")
        {
            return Err(StorageError::Storage(
                "unexpected Store acknowledgement create".to_string(),
            ));
        }
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        read: crate::sync::test_helpers::ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        self.protocol_read_calls().fetch_add(1, Ordering::SeqCst);
        if read == crate::sync::test_helpers::ProtocolRead::Object
            && semantic_prefix.starts_with("store-v1/candidates/")
            && semantic_prefix.contains("/packages/")
        {
            if let Self::InjectHostWrite {
                db,
                write_sql,
                fired,
                ..
            } = self
            {
                if !fired.swap(true, Ordering::SeqCst) {
                    db.execute_test_host_write(write_sql).await;
                }
            }
        }
        Ok(())
    }

    async fn before_blob_create(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        if let Self::RejectBlobCreate {
            reject_create_call,
            create_calls,
            attempted,
            ..
        } = self
        {
            attempted
                .lock()
                .expect("attempted blob record lock")
                .push(blob.clone());
            let call = create_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if *reject_create_call == call {
                return Err(StorageError::Storage(format!(
                    "unexpected blob create call {call}"
                )));
            }
        }
        Ok(())
    }
}

struct InterceptedCycle<'a> {
    storage: &'a CycleStorageInterceptor,
    device: TestDevice,
}

impl<'a> InterceptedCycle<'a> {
    fn new(storage: &'a CycleStorageInterceptor, device: TestDevice) -> Self {
        Self { storage, device }
    }

    async fn run(&self) {
        self.storage
            .run_sync_cycle(&self.device)
            .await
            .expect("cycle");
    }
}

struct SamePrincipalApprovalFixture<'storage> {
    _pending_dir: tempfile::TempDir,
    pending_join: crate::sync::store::PendingDeviceJoinAuthority<'storage>,
    owner: TestDevice,
    approval: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
}

impl<'storage> SamePrincipalApprovalFixture<'storage> {
    async fn prepare(
        owner_db: &Database,
        owner_db_store_dir: coven_foundation::store_dir::StoreDir,
        storage: &'storage TestStore,
        owner: &UserKeypair,
        member: &UserKeypair,
    ) -> Self {
        admit_test_member(
            storage,
            owner_db,
            owner_db_store_dir.clone(),
            owner,
            member,
            &EncryptionService::from_key([59; 32]),
        )
        .await;
        let owner_device = storage
            .bind_device_in(owner_db, owner_db_store_dir.clone(), owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending.sqlite"),
        )
        .expect("open join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin exact device join");
        let pending_join = storage
            .open_pending_device_join(&pending, member, offer)
            .await
            .expect("bind pending Store join");
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare exact provider request");
        let approval = owner_device
            .authorize_device_provider_access(access_request, None)
            .await
            .expect("authorize exact provider access");
        Self {
            _pending_dir: pending_dir,
            pending_join,
            owner: owner_device,
            approval,
        }
    }
}

async fn prepare_cross_principal_approval<'storage>(
    owner_db: &Database,
    owner_db_store_dir: StoreDir,
    storage: &TestStore,
    owner: &UserKeypair,
    member: &UserKeypair,
    pending: &crate::sync::store::DeviceJoinJournalDatabase,
    peer: &'storage CrossPrincipalTestDevice,
) -> (
    crate::sync::store::PendingDeviceJoinAuthority<'storage>,
    TestDevice,
    coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
) {
    let owner_device = storage
        .bind_device_in(owner_db, owner_db_store_dir, owner)
        .await
        .expect("bind owner Store");
    let offer = owner_device
        .begin_device_join(&pubkey_hex(member))
        .await
        .expect("begin exact device join");
    let pending_join = peer
        .open_pending_device_join(pending, member, offer)
        .await
        .expect("bind pending Store join");
    let access_request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("prepare exact provider request");
    let approval = peer
        .authorize_device_provider_access(&owner_device, access_request)
        .await
        .expect("authorize cross-principal provider access");
    assert!(matches!(
        approval.admission,
        coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal { .. }
    ));
    (pending_join, owner_device, approval)
}

mod blob_gates;

mod initialization;

mod host_writes;

mod blob_publication;

mod rotation;

mod write_completion;

mod snapshots;

mod reclamation;

mod joins;

mod join_snapshots;
