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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::blob::{BlobScope, CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::{test_utils::InMemoryCloudHome, CloudHome};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle::{self, run_single_sync_cycle};
use crate::sync::hlc::Hlc;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::SyncStorage;
use crate::sync::store_commit::SnapshotMeta;
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct WriteRevocationRequest {
    producer: crate::sync::device_join::DeviceJoinProducer,
    authority: crate::sync::device_join::ProviderWriteAuthorityRef,
    locator: crate::sync::provider::ProviderAccessLocator,
    protected_slots: Vec<crate::storage::cloud::ObjectSlot>,
}

struct ConfirmedWriteRevocation {
    withdrawal: crate::sync::provider::ProviderAccessWithdrawal,
    requests: Mutex<Vec<WriteRevocationRequest>>,
}

impl ConfirmedWriteRevocation {
    fn direct(locator: crate::sync::provider::ProviderAccessLocator) -> Self {
        Self {
            withdrawal: crate::sync::provider::ProviderAccessWithdrawal::Direct {
                locator,
                verified_absent: true,
            },
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<WriteRevocationRequest> {
        self.requests
            .lock()
            .expect("write-revocation request lock")
            .clone()
    }
}

#[async_trait::async_trait]
impl crate::sync::device_join::DeviceJoinWriteRevocationExecutor for ConfirmedWriteRevocation {
    async fn revoke_write_authority(
        &self,
        producer: crate::sync::device_join::DeviceJoinProducer,
        authority: &crate::sync::device_join::ProviderWriteAuthorityRef,
        locator: &crate::sync::provider::ProviderAccessLocator,
        protected_slots: &[crate::storage::cloud::ObjectSlot],
    ) -> Result<
        crate::sync::provider::ProviderAccessWithdrawal,
        crate::sync::device_join::DeviceJoinError,
    > {
        self.requests
            .lock()
            .expect("write-revocation request lock")
            .push(WriteRevocationRequest {
                producer,
                authority: authority.clone(),
                locator: locator.clone(),
                protected_slots: protected_slots.to_vec(),
            });
        Ok(self.withdrawal.clone())
    }
}

fn cycle_cloud_storage(
    home: Arc<dyn CloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> CloudSyncStorage {
    CloudSyncStorage::new(home, cipher, blob_paths, store_id, keypair)
        .expect("test cloud storage supports immutable copies")
}

async fn cycle_test_store(db: &Database, signer: &UserKeypair) -> TestStore {
    TestStore::create(db, "test-lib", signer.clone())
        .await
        .expect("create exact cycle test Store")
}

#[tokio::test]
async fn serial_store_engine_requires_coordination_before_loading_store_authority() {
    let db = open_serial_test_db();
    let signer = UserKeypair::generate();
    let storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::new(
        cycle_test_store(&db, &signer).await,
    )));

    let error = crate::sync::store_engine::StoreEngine::authorize_borrowed(&*storage, None, &db)
        .await
        .err()
        .expect("Serial cycle engine requires coordination");

    assert!(
        error
            .to_string()
            .contains("coordination capability is absent"),
        "{error}"
    );
    assert_eq!(storage.protocol_read_calls(), 0);
}

#[tokio::test]
async fn store_engine_rejects_local_policy_that_differs_from_verified_root() {
    let source = open_test_db();
    let signer = UserKeypair::generate();
    let store = cycle_test_store(&source, &signer).await;
    let local = open_serial_test_db();
    local
        .install_store_root_authority(store.root.clone(), store.protocol_root.to_bytes())
        .await
        .expect("install exact MergeConcurrent root into Serial database");

    let error = crate::sync::store_engine::StoreEngine::authorize_borrowed(
        &store.storage,
        Some(
            store
                .storage
                .serial_coordination()
                .expect("test Store provides Serial coordination"),
        ),
        &local,
    )
    .await
    .err()
    .expect("verified root policy must match the local database policy");

    assert!(error.to_string().contains("write policy"), "{error}");
    assert!(error.to_string().contains("MergeConcurrent"), "{error}");
    assert!(error.to_string().contains("Serial"), "{error}");
}

#[tokio::test]
async fn serial_post_pull_engine_rejects_partial_materialized_authorization() {
    let db = open_serial_test_db();
    let signer = UserKeypair::generate();
    let store = cycle_test_store(&db, &signer).await;
    let engine = crate::sync::store_engine::StoreEngine::authorize_borrowed(
        &store.storage,
        Some(
            store
                .storage
                .serial_coordination()
                .expect("test Store provides Serial coordination"),
        ),
        &db,
    )
    .await
    .expect("authorize Serial cycle engine");
    host_exec(
        &db,
        "DELETE FROM protocol_state WHERE key = 'serial_membership_state'",
    )
    .await;

    let error = engine
        .after_pull()
        .await
        .err()
        .expect("partial Serial authorization must fail the cycle");

    assert!(
        error
            .to_string()
            .contains("read materialized Serial membership"),
        "{error}"
    );
    assert!(error.to_string().contains("partially durable"), "{error}");
}

async fn recover_serial_owner_state(
    storage: &CloudSyncStorage,
    source: &Database,
    store_id: &str,
    root: &crate::sync::store_commit::StoreRootRef,
    owner: &UserKeypair,
) -> (tempfile::TempDir, Database, String) {
    let image_dir = tempfile::tempdir().expect("create Serial snapshot image directory");
    let image_path = image_dir.path().to_path_buf();
    let tables = test_synced_tables();
    let image = source
        .call(move |connection| {
            crate::sync::snapshot::create_snapshot(connection, &image_path, &tables)
                .map_err(|error| crate::database::DbError::Message(error.to_string()))
        })
        .await
        .expect("create Serial bootstrap snapshot image");
    publish_snapshot_fixture(
        storage,
        root,
        image,
        crate::sync::store_commit::CommitFrontier::Serial(None),
        owner,
        None,
        source,
    )
    .await
    .expect("publish Serial bootstrap snapshot");
    crate::sync::store_engine::stage_serial_acknowledgement_for_test(
        source,
        storage,
        storage
            .serial_coordination()
            .expect("Serial bootstrap coordination"),
        crate::sync::store_commit::CommitFrontier::Serial(None),
        "2024-01-01T00:00:01Z".to_string(),
        owner,
    )
    .await
    .expect("stage Serial bootstrap stability acknowledgement");
    crate::sync::store_engine::drain_serial_acknowledgements_for_test(
        source,
        storage,
        storage
            .serial_coordination()
            .expect("Serial bootstrap coordination"),
        owner,
    )
    .await
    .expect("activate Serial bootstrap stability acknowledgement");
    let target_dir = tempfile::tempdir().expect("create Serial bootstrap destination");
    let target_path = target_dir.path().join("store.sqlite");
    let bootstrap = crate::sync::snapshot::bootstrap_from_snapshot(
        storage,
        Some(
            storage
                .serial_coordination()
                .expect("Serial bootstrap coordination"),
        ),
        store_id,
        root.clone(),
        &crate::join_code::MembershipFloor::Serial(None),
        source.schema_version(),
        &target_path,
    )
    .await
    .expect("verify Serial bootstrap snapshot");
    let target = bootstrap
        .open_database(
            store_id,
            &target_path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            "recovering-owner".to_string(),
            &test_migrations(),
        )
        .await
        .expect("open Serial bootstrap database");
    let protocol = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .expect("load exact Serial Store for Owner recovery")
        .value;
    crate::sync::cycle::ensure_serial_founder_authorization(storage, &target, root, &protocol)
        .await
        .expect("install exact Serial founder authorization");
    crate::sync::store_engine::pull_store_commits(
        &target,
        target.synced_tables(),
        storage,
        Some(
            storage
                .serial_coordination()
                .expect("Serial bootstrap coordination"),
        ),
        root.store_root_hash,
        &StoreDir::new(target_dir.path()),
        None,
        None,
    )
    .await
    .expect("pull accepted Serial commits after bootstrap snapshot");
    let owner_grant = protocol.descriptor.founder_grant.clone();
    let activation = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &pubkey_hex(owner),
        &owner_grant,
        &protocol.descriptor.founder_recovery,
    )
    .expect("derive exact founder recovery activation");
    let authority = crate::sync::restore_code::OwnerRecoveryAuthority {
        owner_identity_secret: hex::encode(owner.to_keypair_bytes()),
        owner_grant: owner_grant.clone(),
        recovery: crate::sync::store_commit::OwnerRecoveryCursor {
            owner_grant,
            position: crate::sync::store_commit::OwnerRecoveryPosition::BeforeFirst { activation },
        },
        published_at: T0.to_string(),
    };
    let device_id = crate::sync::store_registration::recover_owner_device_serial(
        &target,
        storage,
        storage
            .serial_coordination()
            .expect("Serial test coordination"),
        owner,
        &authority,
    )
    .await
    .expect("recover exact Serial Owner device")
    .device_id
    .to_string();
    (target_dir, target, device_id)
}

/// Run one sync cycle for device "M" with no cloud home (no outbox drain).
async fn run_cycle_m(
    storage: &TestStore,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    run_cycle_m_result(storage, db, cipher, keypair, hlc, ld)
        .await
        .expect("cycle");
}

async fn run_cycle_m_result(
    storage: &TestStore,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) -> Result<(), String> {
    storage.open_into(db).await.expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize MergeConcurrent test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "exact test Store has no local device id".to_string())?;
    run_single_sync_cycle(
        &storage.storage,
        &device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        &PendingRotation::none(),
        keypair,
        None,
        ld,
        None,
        None,
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle_in_task(
    storage: Arc<CycleStorageInterceptor>,
    db: Database,
    cipher: Arc<RwLock<CloudCipher>>,
    keypair: UserKeypair,
    hlc: Arc<Hlc>,
    store_dir: StoreDir,
    device_id: String,
) -> Result<(), cycle::SyncCycleFailure> {
    tokio::spawn(async move {
        run_single_sync_cycle(
            storage.as_ref(),
            &device_id,
            hlc.as_ref(),
            &SystemClock,
            &db,
            cipher.as_ref(),
            &PendingRotation::none(),
            &keypair,
            None,
            &store_dir,
            None,
            None,
        )
        .await
        .map(|_| ())
    })
    .await
    .expect("cycle task completes")
}

#[tokio::test]
async fn tombstone_provider_failure_fails_cycle_and_preserves_intent() {
    let db = open_test_db();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    storage.open_into(&db).await.expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize MergeConcurrent membership");
    let stored = create_exact_blob(&storage, "photos", "maintenance", b"maintenance").await;
    db.call(move |conn| Database::enqueue_delete_on(conn, &stored, T0))
        .await
        .expect("queue exact maintenance tombstone");
    let home = InMemoryCloudHome::new();
    home.arm_write_failures();
    let (_temp, store_dir) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Plaintext);
    let result = run_single_sync_cycle(
        &storage.storage,
        "M",
        &Hlc::new("M".to_string()),
        &SystemClock,
        &db,
        &cipher,
        &PendingRotation::none(),
        &keypair,
        None,
        &store_dir,
        Some(&home),
        None,
    )
    .await;
    let error = result.expect_err("tombstone publication failure fails the cycle");
    assert!(error.contains("drain queued blob tombstones"), "{error}");
    assert_eq!(
        db.get_pending_cloud_deletes().await.unwrap().len(),
        1,
        "failed maintenance remains queued"
    );
}

async fn store_package_exists(
    db: &Database,
    storage: &TestStore,
    stream_id: &str,
    sequence: u64,
) -> bool {
    let Some((reference, commit)) =
        load_exact_materialized_commit(db, &storage.storage, stream_id, sequence)
            .await
            .expect("load exact materialized Store commit")
    else {
        return false;
    };
    match crate::sync::store_objects::load_store_package(
        &storage.storage,
        &reference,
        &commit.value,
    )
    .await
    {
        Ok(package) => package.is_some(),
        Err(crate::sync::store_objects::StoreObjectError::Storage(
            crate::sync::storage::StorageError::NotFound(_),
        )) => false,
        Err(error) => panic!("load Store package: {error}"),
    }
}

async fn local_store_stream_id(db: &Database) -> String {
    let local_device = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let (registration_ref, registration) = db
        .activated_store_device_registration_records()
        .await
        .expect("read activated Store registrations")
        .into_iter()
        .find(|(_, registration)| registration.device_id.to_string() == local_device)
        .expect("local Store registration is active");
    registration
        .store_announcement_activation(&registration_ref)
        .expect("derive local Store announcement activation")
        .author_stream_id()
        .to_string()
}

async fn local_store_package_exists(db: &Database, storage: &TestStore, sequence: u64) -> bool {
    let stream_id = local_store_stream_id(db).await;
    store_package_exists(db, storage, &stream_id, sequence).await
}

async fn retain_store_packages_for_assertion(db: &Database, storage: &TestStore, marker: &[u8]) {
    let membership = storage
        .open_into(db)
        .await
        .expect("open exact Store before seeding snapshot");
    crate::sync::store_snapshot::push_store_snapshot(
        &storage.storage,
        storage.store_root_hash(),
        crate::sync::snapshot::CreatedSnapshot {
            db_image: marker.to_vec(),
            blobs: Vec::new(),
        },
        crate::sync::store_commit::CommitFrontier::MergeConcurrent(BTreeMap::new()),
        db.schema_version(),
        &storage.protocol_founder_keypair(),
        T0.to_string(),
        Some(&membership),
        db,
    )
    .await
    .expect("publish exact Store snapshot fixture");
}

async fn latest_store_snapshot_meta(db: &Database) -> Option<SnapshotMeta> {
    db.latest_local_store_snapshot()
        .await
        .expect("read latest exact Store snapshot")
        .map(|snapshot| snapshot.meta)
}

async fn create_exact_blob(
    storage: &TestStore,
    namespace: &str,
    id: &str,
    bytes: &[u8],
) -> crate::blob::locator::StoredBlobRef {
    let (uploader, registration, _) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder device authority");
    let authority = crate::sync::storage::BlobWriteAuthority::new(&uploader, &registration)
        .expect("validate exact blob write authority");
    let protection = EncryptionService::from_key([42; 32]);
    let locator = crate::blob::locator::BlobLocator::opaque(
        namespace,
        id,
        uploader.clone(),
        crate::blob::locator::RemoteAudience::Store,
        BlobScope::Master,
        protection.seal_key_fingerprint(),
        bytes.len() as u64,
        crate::sync::store_commit::ObjectHash::digest(bytes),
    )
    .expect("build exact blob locator");
    let temp = tempfile::tempdir().expect("create exact blob directory");
    let plaintext = temp.path().join("plaintext");
    let spool = temp.path().join("stored");
    crate::local_blob::write_atomic(&plaintext, bytes)
        .await
        .expect("write exact blob plaintext");
    let slot = storage
        .storage
        .allocate_blob_slot(&locator, &authority)
        .await
        .expect("allocate exact blob slot");
    storage
        .storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            crate::sync::storage::BlobSpoolProtection::Opaque(protection),
            &plaintext,
            &spool,
        )
        .await
        .expect("seal exact blob");
    let stored = storage
        .storage
        .prepare_blob_object(&locator, &authority, slot, &spool)
        .await
        .expect("prepare exact blob");
    let progress = crate::storage::cloud::no_progress();
    storage
        .storage
        .create_blob_object_from_file(&stored, &authority, &spool, &progress)
        .await
        .expect("create exact blob");
    stored
}

fn fail_exact_create_on(storage: &TestStore, call: usize) {
    storage.home.fail_exact_create_before_call(call);
}

async fn stored_blob_for_row(
    db: &Database,
    table: &str,
    row_id: &str,
) -> Option<crate::blob::locator::StoredBlobRef> {
    db.row_blob_ref(table, row_id)
        .await
        .expect("resolve exact blob row")
        .stored()
        .cloned()
}

async fn stored_blob_exists(db: &Database, storage: &TestStore, table: &str, row_id: &str) -> bool {
    let Some(stored) = stored_blob_for_row(db, table, row_id).await else {
        return false;
    };
    storage.storage.verify_blob_object(&stored).await.is_ok()
}

async fn activate_joined_test_device(
    storage: &TestStore,
    owner_db: &Database,
    joining_db: &Database,
    joining_identity: &UserKeypair,
) {
    crate::sync::test_helpers::install_active_device_fixture(
        storage,
        owner_db,
        joining_db,
        joining_identity,
        T0,
    )
    .await
    .expect("install active exact device fixture");
}

fn exercise_pre_attempt_abandonment<'a>(
    owner_db: &'a Database,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::device_join::{DeviceJoinRole, DeviceJoinStatus};

        let coordination = if owner_db.write_policy() == crate::WritePolicy::Serial {
            Some(
                storage
                    .storage
                    .serial_coordination()
                    .expect("Serial test Store has coordination"),
            )
        } else {
            None
        };
        let authorization = Box::new(match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                crate::sync::device_join::DeviceJoinAuthorization::MergeConcurrent(
                    storage
                        .open_into(owner_db)
                        .await
                        .expect("load exact Merge membership"),
                )
            }
            crate::WritePolicy::Serial => {
                crate::sync::device_join::DeviceJoinAuthorization::Serial(
                    crate::sync::store_engine::serial::publication::current_serial_authorization(
                        owner_db,
                        &storage.storage,
                        coordination.expect("Serial test Store has coordination"),
                    )
                    .await
                    .expect("load exact Serial authorization"),
                )
            }
        });
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::device_join::begin_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                &pubkey_hex(member),
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ))
            .await
            .expect("begin exact device join"),
        );
        let _request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_provider_access_request(
                    &pending,
                    crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                        .await
                        .expect("resolve provider binding"),
                    member,
                    (*offer).clone(),
                ),
            )
            .await
            .expect("prepare exact provider access request"),
        );
        let abandonment = Box::new(
            Box::pin(crate::sync::device_join::abandon_device_join(
                owner_db,
                &storage.storage,
                coordination,
                &authorization,
                owner,
                (*offer).clone(),
            ))
            .await
            .expect("abandon device join before attempt activation"),
        );
        let retried = Box::pin(crate::sync::device_join::abandon_device_join(
            owner_db,
            &storage.storage,
            coordination,
            &authorization,
            owner,
            *offer,
        ))
        .await
        .expect("retry device join abandonment");
        assert_eq!(retried, *abandonment);

        let observed = Box::pin(crate::sync::device_join::observe_device_join_abandonment(
            &pending,
            &storage.storage,
            &storage.root,
            (*abandonment).clone(),
        ))
        .await
        .expect("observe exact abandonment");
        let observed_retry = Box::pin(crate::sync::device_join::observe_device_join_abandonment(
            &pending,
            &storage.storage,
            &storage.root,
            (*abandonment).clone(),
        ))
        .await
        .expect("retry exact abandonment observation");
        assert_eq!(observed_retry, observed);
        assert!(matches!(
            crate::sync::device_join::load_store_device_join_status(
                owner_db,
                abandonment.abandonment.attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load owner join status"),
            Some(DeviceJoinStatus::Abandoned { abandonment: durable }) if durable == *abandonment
        ));
        assert!(matches!(
            crate::sync::device_join::load_pending_device_join_status(
                &pending,
                abandonment.abandonment.attempt_id,
            )
            .expect("load joiner join status"),
            Some(DeviceJoinStatus::Abandoned { abandonment: durable }) if durable == *abandonment
        ));
    })
}

#[derive(Clone, Copy)]
enum JoinerCancellationDisposition {
    Closure,
    WriteRevocation,
}

#[derive(Clone, Copy)]
enum ExactCreateInterruption {
    BeforeVisibility,
    AfterVisibility,
}

fn exercise_provider_access_grant_create_interruption<'a>(
    owner_db: &'a Database,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    interruption: ExactCreateInterruption,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::device_join::{DeviceJoinRole, DeviceJoinStatus};

        let coordination = if owner_db.write_policy() == crate::WritePolicy::Serial {
            Some(
                storage
                    .storage
                    .serial_coordination()
                    .expect("Serial test Store has coordination"),
            )
        } else {
            None
        };
        let authorization = Box::new(match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                crate::sync::device_join::DeviceJoinAuthorization::MergeConcurrent(
                    storage
                        .open_into(owner_db)
                        .await
                        .expect("load exact Merge membership"),
                )
            }
            crate::WritePolicy::Serial => {
                crate::sync::device_join::DeviceJoinAuthorization::Serial(
                    crate::sync::store_engine::serial::publication::current_serial_authorization(
                        owner_db,
                        &storage.storage,
                        coordination.expect("Serial test Store has coordination"),
                    )
                    .await
                    .expect("load exact Serial authorization"),
                )
            }
        });
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::device_join::begin_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                &pubkey_hex(member),
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ))
            .await
            .expect("begin exact device join"),
        );
        let attempt_id = offer.attempt_id;
        let request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_provider_access_request(
                    &pending,
                    crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                        .await
                        .expect("resolve provider binding"),
                    member,
                    *offer,
                ),
            )
            .await
            .expect("prepare exact provider access request"),
        );
        match interruption {
            ExactCreateInterruption::BeforeVisibility => {
                storage.home.fail_exact_create_before_call(1)
            }
            ExactCreateInterruption::AfterVisibility => {
                storage.home.fail_exact_create_after_call(1)
            }
        }
        let first = Box::pin(crate::sync::device_join::authorize_device_provider_access(
            owner_db,
            &storage.storage,
            coordination,
            None,
            None,
            &authorization,
            owner,
            (*request).clone(),
        ))
        .await;
        let approval = match interruption {
            ExactCreateInterruption::BeforeVisibility => {
                assert!(
                    first.is_err(),
                    "the injected create fails before visibility"
                );
                assert!(matches!(
                    crate::sync::device_join::load_store_device_join_status(
                        owner_db,
                        attempt_id,
                        DeviceJoinRole::ProviderAdministrator,
                    )
                    .await
                    .expect("load provider create status"),
                    Some(DeviceJoinStatus::ProviderAccessGrantCreatePending { .. })
                ));
                Box::pin(crate::sync::device_join::authorize_device_provider_access(
                    owner_db,
                    &storage.storage,
                    coordination,
                    None,
                    None,
                    &authorization,
                    owner,
                    *request,
                ))
                .await
                .expect("resume provider access grant creation")
            }
            ExactCreateInterruption::AfterVisibility => {
                first.expect("lost create response settles through exact readback")
            }
        };
        let retry = Box::pin(crate::sync::device_join::authorize_device_provider_access(
            owner_db,
            &storage.storage,
            coordination,
            None,
            None,
            &authorization,
            owner,
            (*approval.request).clone(),
        ))
        .await
        .expect("retry completed provider access authorization");
        assert_eq!(retry, approval);
        assert!(matches!(
            crate::sync::device_join::load_store_device_join_status(
                owner_db,
                attempt_id,
                DeviceJoinRole::ProviderAdministrator,
            )
            .await
            .expect("load completed provider access status"),
            Some(DeviceJoinStatus::AwaitingRegistrationRequest { .. })
        ));
    })
}

fn exercise_post_attempt_cancellation<'a>(
    owner_db: &'a Database,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    joiner_disposition: JoinerCancellationDisposition,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::device_join::{
            DeviceJoinRole, DeviceJoinStatus, JoinerJoinTerminal, ProviderAdminJoinTerminal,
        };

        let coordination = if owner_db.write_policy() == crate::WritePolicy::Serial {
            Some(
                storage
                    .storage
                    .serial_coordination()
                    .expect("Serial test Store has coordination"),
            )
        } else {
            None
        };
        let authorization = Box::new(match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                crate::sync::device_join::DeviceJoinAuthorization::MergeConcurrent(
                    storage
                        .open_into(owner_db)
                        .await
                        .expect("load exact Merge membership"),
                )
            }
            crate::WritePolicy::Serial => {
                crate::sync::device_join::DeviceJoinAuthorization::Serial(
                    crate::sync::store_engine::serial::publication::current_serial_authorization(
                        owner_db,
                        &storage.storage,
                        coordination.expect("Serial test Store has coordination"),
                    )
                    .await
                    .expect("load exact Serial authorization"),
                )
            }
        });
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::device_join::begin_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                &pubkey_hex(member),
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ))
            .await
            .expect("begin exact device join"),
        );
        let access_request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_provider_access_request(
                    &pending,
                    crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                        .await
                        .expect("resolve provider binding"),
                    member,
                    *offer,
                ),
            )
            .await
            .expect("prepare exact provider access request"),
        );
        let approval = Box::new(
            Box::pin(crate::sync::device_join::authorize_device_provider_access(
                owner_db,
                &storage.storage,
                coordination,
                None,
                None,
                &authorization,
                owner,
                *access_request,
            ))
            .await
            .expect("authorize exact provider access"),
        );
        let joiner_access_locator = approval.access_grant.grant.locator.clone();
        let registration_request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_registration_request(
                    &pending,
                    &storage.storage,
                    coordination,
                    None,
                    member,
                    *approval,
                ),
            )
            .await
            .expect("prepare exact registration request"),
        );
        let provisional = Box::new(
            Box::pin(
                crate::sync::device_join::accept_device_registration_request(
                    owner_db,
                    &storage.storage,
                    coordination,
                    &authorization,
                    owner,
                    *registration_request,
                ),
            )
            .await
            .expect("activate exact join attempt"),
        );
        let attempt_id = provisional.publication_authorization.attempt.attempt_id;
        let cancellation = Box::new(
            Box::pin(crate::sync::device_join::cancel_device_join(
                owner_db,
                &storage.storage,
                coordination,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel exact active join attempt"),
        );
        let cancellation_retry = Box::pin(crate::sync::device_join::cancel_device_join(
            owner_db,
            &storage.storage,
            coordination,
            &authorization,
            owner,
            provisional.publication_authorization.attempt.clone(),
        ))
        .await
        .expect("retry exact active join cancellation");
        assert_eq!(cancellation_retry, *cancellation);

        let administrator_terminal = Box::new(
            Box::pin(crate::sync::device_join::close_device_provider_admission(
                owner_db,
                &storage.storage,
                None,
                owner,
                (*cancellation).clone(),
            ))
            .await
            .expect("close exact provider admission"),
        );
        let administrator_retry =
            Box::pin(crate::sync::device_join::close_device_provider_admission(
                owner_db,
                &storage.storage,
                None,
                owner,
                (*cancellation).clone(),
            ))
            .await
            .expect("retry exact provider admission closure");
        assert_eq!(administrator_retry, *administrator_terminal);
        assert!(matches!(
            administrator_terminal.as_ref(),
            ProviderAdminJoinTerminal::Cancelled(_)
        ));

        let joiner_revocation = ConfirmedWriteRevocation::direct(joiner_access_locator.clone());
        let joiner_terminal = Box::new(match joiner_disposition {
            JoinerCancellationDisposition::Closure => {
                Box::pin(crate::sync::device_join::close_joining_device(
                    &pending,
                    &storage.storage,
                    storage.home.as_ref(),
                    &storage.root,
                    member,
                    (*cancellation).clone(),
                ))
                .await
                .expect("close exact joining device")
            }
            JoinerCancellationDisposition::WriteRevocation => {
                Box::pin(crate::sync::device_join::revoke_joining_device_writes(
                    owner_db,
                    &storage.storage,
                    &authorization,
                    owner,
                    (*cancellation).clone(),
                    &joiner_revocation,
                    storage
                        .protocol_root
                        .descriptor
                        .founder_provider_admin
                        .grant_id
                        .clone(),
                ))
                .await
                .expect("revoke absent joining-device writes")
            }
        });
        if matches!(
            joiner_disposition,
            JoinerCancellationDisposition::WriteRevocation
        ) {
            let crate::sync::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack,
            } = &provisional.request.expected_registration.acknowledgements
            else {
                panic!("joining registration has a non-Store acknowledgement stream");
            };
            let mut expected_slots = vec![
                provisional.request.registration_slot.clone(),
                first_ack.clone(),
            ];
            if let crate::sync::device_join::DeviceProviderResponseReservation::CrossPrincipal {
                response_slot,
            } = &provisional.request.response
            {
                expected_slots.push(response_slot.clone());
            }
            assert_eq!(
                joiner_revocation.requests(),
                vec![WriteRevocationRequest {
                    producer: crate::sync::device_join::DeviceJoinProducer::Joiner,
                    authority: crate::sync::device_join::ProviderWriteAuthorityRef::MemberAccess(
                        provisional.request.approval.access_grant.grant_ref.clone(),
                    ),
                    locator: joiner_access_locator.clone(),
                    protected_slots: expected_slots,
                }],
            );
        }
        let joiner_retry = match joiner_disposition {
            JoinerCancellationDisposition::Closure => {
                Box::pin(crate::sync::device_join::close_joining_device(
                    &pending,
                    &storage.storage,
                    storage.home.as_ref(),
                    &storage.root,
                    member,
                    (*cancellation).clone(),
                ))
                .await
                .expect("retry exact joining-device closure")
            }
            JoinerCancellationDisposition::WriteRevocation => {
                let revocation = ConfirmedWriteRevocation::direct(joiner_access_locator);
                let terminal = Box::pin(crate::sync::device_join::revoke_joining_device_writes(
                    owner_db,
                    &storage.storage,
                    &authorization,
                    owner,
                    (*cancellation).clone(),
                    &revocation,
                    storage
                        .protocol_root
                        .descriptor
                        .founder_provider_admin
                        .grant_id
                        .clone(),
                ))
                .await
                .expect("retry absent joining-device write revocation");
                assert!(revocation.requests().is_empty());
                terminal
            }
        };
        assert_eq!(joiner_retry, *joiner_terminal);
        assert!(match joiner_disposition {
            JoinerCancellationDisposition::Closure =>
                matches!(joiner_terminal.as_ref(), JoinerJoinTerminal::Cancelled(_)),
            JoinerCancellationDisposition::WriteRevocation => matches!(
                joiner_terminal.as_ref(),
                JoinerJoinTerminal::WriteRevoked(_)
            ),
        });
        assert!(
            crate::sync::device_join::load_store_device_join_actions(owner_db)
                .await
                .expect("enumerate terminal Store join actions")
                .contains(
                    &crate::sync::device_join::DeviceJoinAction::TransferProviderAdminTerminal(
                        (*administrator_terminal).clone(),
                    ),
                )
        );
        let joiner_action = crate::sync::device_join::DeviceJoinAction::TransferJoinerTerminal(
            (*joiner_terminal).clone(),
        );
        match joiner_disposition {
            JoinerCancellationDisposition::Closure => assert_eq!(
                pending
                    .actions()
                    .expect("enumerate terminal joiner actions"),
                vec![joiner_action],
            ),
            JoinerCancellationDisposition::WriteRevocation => assert!(
                crate::sync::device_join::load_store_device_join_actions(owner_db)
                    .await
                    .expect("enumerate replacement joiner terminal")
                    .contains(&joiner_action),
            ),
        }

        storage.home.fail_exact_create_before_call(1);
        let interrupted_cleanup = Box::pin(crate::sync::device_join::prepare_device_join_cleanup(
            owner_db,
            &storage.storage,
            coordination,
            storage.home.as_ref(),
            &authorization,
            owner,
            (*cancellation).clone(),
            (*administrator_terminal).clone(),
            (*joiner_terminal).clone(),
        ))
        .await;
        assert!(
            interrupted_cleanup.is_err(),
            "the cleanup-receipt create interruption surfaces"
        );
        assert!(matches!(
            crate::sync::device_join::load_store_device_join_status(
                owner_db,
                attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load interrupted cleanup status"),
            Some(DeviceJoinStatus::CleanupReceiptCreatePending { .. })
        ));
        let receipt = Box::new(
            Box::pin(crate::sync::device_join::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
                coordination,
                storage.home.as_ref(),
                &authorization,
                owner,
                (*cancellation).clone(),
                (*administrator_terminal).clone(),
                (*joiner_terminal).clone(),
            ))
            .await
            .expect("resume exact cleanup receipt"),
        );
        let receipt_retry = Box::pin(crate::sync::device_join::prepare_device_join_cleanup(
            owner_db,
            &storage.storage,
            coordination,
            storage.home.as_ref(),
            &authorization,
            owner,
            (*cancellation).clone(),
            *administrator_terminal,
            *joiner_terminal,
        ))
        .await
        .expect("retry exact cleanup receipt");
        assert_eq!(receipt_retry, *receipt);

        let activation = Box::new(
            Box::pin(crate::sync::device_join::activate_device_join_cleanup(
                owner_db,
                &storage.storage,
                coordination,
                &authorization,
                owner,
                attempt_id,
                (*receipt).clone(),
            ))
            .await
            .expect("activate exact cleanup receipt"),
        );
        let activation_retry = Box::pin(crate::sync::device_join::activate_device_join_cleanup(
            owner_db,
            &storage.storage,
            coordination,
            &authorization,
            owner,
            attempt_id,
            *receipt,
        ))
        .await
        .expect("retry exact cleanup activation");
        assert_eq!(activation_retry, *activation);

        let owner_complete = crate::sync::device_join::complete_owner_device_join_cleanup(
            owner_db,
            attempt_id,
            (*activation).clone(),
        )
        .await
        .expect("complete exact owner cleanup");
        let owner_complete_retry = crate::sync::device_join::complete_owner_device_join_cleanup(
            owner_db,
            attempt_id,
            (*activation).clone(),
        )
        .await
        .expect("retry exact owner cleanup completion");
        assert_eq!(owner_complete_retry, owner_complete);
        let mut forged_activation = (*activation).clone();
        forged_activation.activation.commit_hash =
            crate::sync::store_commit::ObjectHash::digest(b"forged cleanup activation");
        assert!(
            crate::sync::device_join::accept_joiner_device_join_cleanup(
                &pending,
                &storage.storage,
                &storage.root,
                forged_activation,
            )
            .await
            .is_err(),
            "joiner cleanup must reject an activation whose exact Store commit was not verified",
        );
        crate::sync::device_join::accept_joiner_device_join_cleanup(
            &pending,
            &storage.storage,
            &storage.root,
            (*activation).clone(),
        )
        .await
        .expect("accept exact joiner cleanup activation");
        let joiner_complete = crate::sync::device_join::complete_joiner_device_join_cleanup(
            &pending,
            (*activation).clone(),
        )
        .expect("complete exact joiner cleanup");
        let joiner_complete_retry =
            crate::sync::device_join::complete_joiner_device_join_cleanup(&pending, *activation)
                .expect("retry exact joiner cleanup completion");
        assert_eq!(joiner_complete_retry, joiner_complete);
        assert!(matches!(
            crate::sync::device_join::load_store_device_join_status(
                owner_db,
                attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load owner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
        assert!(matches!(
            crate::sync::device_join::load_pending_device_join_status(&pending, attempt_id)
                .expect("load joiner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
    })
}

fn exercise_missing_provider_administrator<'a>(
    owner_db: &'a Database,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::device_join::{JoinerJoinTerminal, ProviderAdminJoinTerminal};
        use crate::sync::storage::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
        };

        let authorization = Box::new(
            crate::sync::device_join::DeviceJoinAuthorization::MergeConcurrent(
                storage
                    .open_into(owner_db)
                    .await
                    .expect("load exact Merge membership"),
            ),
        );
        let crate::sync::storage::StoreProviderBinding::Dropbox { namespace_id } =
            &storage.protocol_root.descriptor.provider
        else {
            panic!("cross-principal test Store is not Dropbox");
        };
        let peer_home = std::sync::Arc::new(storage.home.as_ref().clone().with_provider_binding(
            ResolvedProviderBinding {
                store: storage.protocol_root.descriptor.provider.clone(),
                device: ProviderDeviceBinding {
                    principal: ProviderPrincipalId::Dropbox {
                        account_id: "member-account".to_string(),
                    },
                },
            },
        ));
        let peer_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
            peer_home.clone(),
            crate::sync::cloud_storage::CloudCipher::Encrypted(
                crate::encryption::EncryptionService::from_key([42; 32]),
            ),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            "cross-principal-revocation-store",
            member.clone(),
        )
        .expect("create peer exact storage")
        .with_test_serial_coordination(peer_home.clone());
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::device_join::begin_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                &pubkey_hex(member),
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ))
            .await
            .expect("begin cross-principal device join"),
        );
        let provider_locator = offer.provider_admin.access.clone();
        let request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_provider_access_request(
                    &pending,
                    crate::sync::storage::SyncStorage::provider_binding(&peer_storage)
                        .await
                        .expect("resolve peer provider binding"),
                    member,
                    *offer,
                ),
            )
            .await
            .expect("prepare cross-principal access request"),
        );
        let access_administrator = crate::sync::test_helpers::TestDropboxAccessAdministrator {
            namespace_id: namespace_id.clone(),
        };
        let approval = Box::new(
            Box::pin(crate::sync::device_join::authorize_device_provider_access(
                owner_db,
                &storage.storage,
                None,
                Some(storage.home.as_ref()),
                Some(&access_administrator),
                &authorization,
                owner,
                *request,
            ))
            .await
            .expect("authorize cross-principal provider access"),
        );
        let registration_request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_registration_request(
                    &pending,
                    &peer_storage,
                    None,
                    Some(peer_home.as_ref()),
                    member,
                    *approval,
                ),
            )
            .await
            .expect("prepare cross-principal registration request"),
        );
        let provisional = Box::new(
            Box::pin(
                crate::sync::device_join::accept_device_registration_request(
                    owner_db,
                    &storage.storage,
                    None,
                    &authorization,
                    owner,
                    *registration_request,
                ),
            )
            .await
            .expect("activate cross-principal join attempt"),
        );
        let attempt_id = provisional.publication_authorization.attempt.attempt_id;
        let cancellation = Box::new(
            Box::pin(crate::sync::device_join::cancel_device_join(
                owner_db,
                &storage.storage,
                None,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel cross-principal join attempt"),
        );
        owner_db
            .call(|connection| {
                connection
                    .execute(
                        "DELETE FROM protocol_state
                         WHERE key GLOB 'device_join/*/provider_administrator'",
                        [],
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("remove unavailable provider administrator's local journal");
        let revocation = ConfirmedWriteRevocation::direct(provider_locator.clone());
        let administrator_terminal = Box::new(
            Box::pin(
                crate::sync::device_join::revoke_device_provider_admission_writes(
                    owner_db,
                    &storage.storage,
                    &authorization,
                    owner,
                    (*cancellation).clone(),
                    &revocation,
                    storage
                        .protocol_root
                        .descriptor
                        .founder_provider_admin
                        .grant_id
                        .clone(),
                ),
            )
            .await
            .expect("revoke absent provider-administrator writes"),
        );
        let crate::sync::device_join::DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
            &provisional.request.approval.admission
        else {
            panic!("missing-provider test did not create a cross-principal challenge");
        };
        assert_eq!(
            revocation.requests(),
            vec![WriteRevocationRequest {
                producer: crate::sync::device_join::DeviceJoinProducer::ProviderAdministrator,
                authority:
                    crate::sync::device_join::ProviderWriteAuthorityRef::ProviderAdministrator(
                        provisional
                            .request
                            .approval
                            .request
                            .offer
                            .provider_admin
                            .grant_id
                            .clone(),
                    ),
                locator: provider_locator.clone(),
                protected_slots: vec![challenge.administrator_object.slot.clone()],
            }],
        );
        let retry_revocation = ConfirmedWriteRevocation::direct(provider_locator);
        let administrator_retry = Box::pin(
            crate::sync::device_join::revoke_device_provider_admission_writes(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                (*cancellation).clone(),
                &retry_revocation,
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ),
        )
        .await
        .expect("retry provider-administrator write revocation");
        assert!(retry_revocation.requests().is_empty());
        assert_eq!(administrator_retry, *administrator_terminal);
        assert!(matches!(
            administrator_terminal.as_ref(),
            ProviderAdminJoinTerminal::WriteRevoked(_)
        ));
        let joiner_terminal = Box::new(
            Box::pin(crate::sync::device_join::close_joining_device(
                &pending,
                &storage.storage,
                peer_home.as_ref(),
                &storage.root,
                member,
                (*cancellation).clone(),
            ))
            .await
            .expect("close cross-principal joining device"),
        );
        assert!(matches!(
            joiner_terminal.as_ref(),
            JoinerJoinTerminal::Cancelled(_)
        ));
        let receipt = Box::new(
            Box::pin(crate::sync::device_join::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
                None,
                storage.home.as_ref(),
                &authorization,
                owner,
                (*cancellation).clone(),
                *administrator_terminal,
                *joiner_terminal,
            ))
            .await
            .expect("prepare cleanup with revoked provider administrator"),
        );
        let activation = Box::new(
            Box::pin(crate::sync::device_join::activate_device_join_cleanup(
                owner_db,
                &storage.storage,
                None,
                &authorization,
                owner,
                attempt_id,
                *receipt,
            ))
            .await
            .expect("activate cleanup with revoked provider administrator"),
        );
        crate::sync::device_join::complete_owner_device_join_cleanup(
            owner_db,
            attempt_id,
            (*activation).clone(),
        )
        .await
        .expect("complete owner cleanup");
        crate::sync::device_join::accept_joiner_device_join_cleanup(
            &pending,
            &storage.storage,
            &storage.root,
            (*activation).clone(),
        )
        .await
        .expect("accept exact joiner cleanup activation");
        crate::sync::device_join::complete_joiner_device_join_cleanup(&pending, *activation)
            .expect("complete joiner cleanup");
    })
}

fn exercise_cancellation_against_inflight_registration<'a>(
    owner_db: &'a Database,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::device_join::DeviceJoinAuthorization;

        let coordination = if owner_db.write_policy() == crate::WritePolicy::Serial {
            Some(
                storage
                    .storage
                    .serial_coordination()
                    .expect("Serial test Store has coordination"),
            )
        } else {
            None
        };
        let authorization = Box::new(match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => DeviceJoinAuthorization::MergeConcurrent(
                storage
                    .open_into(owner_db)
                    .await
                    .expect("load exact Merge membership"),
            ),
            crate::WritePolicy::Serial => DeviceJoinAuthorization::Serial(
                crate::sync::store_engine::serial::publication::current_serial_authorization(
                    owner_db,
                    &storage.storage,
                    coordination.expect("Serial test Store has coordination"),
                )
                .await
                .expect("load exact Serial authorization"),
            ),
        });
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::device_join::begin_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                &pubkey_hex(member),
                storage
                    .protocol_root
                    .descriptor
                    .founder_provider_admin
                    .grant_id
                    .clone(),
            ))
            .await
            .expect("begin exact device join"),
        );
        let access_request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_provider_access_request(
                    &pending,
                    crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                        .await
                        .expect("resolve provider binding"),
                    member,
                    *offer,
                ),
            )
            .await
            .expect("prepare exact provider access request"),
        );
        let approval = Box::new(
            Box::pin(crate::sync::device_join::authorize_device_provider_access(
                owner_db,
                &storage.storage,
                coordination,
                None,
                None,
                &authorization,
                owner,
                *access_request,
            ))
            .await
            .expect("authorize exact provider access"),
        );
        let registration_request = Box::new(
            Box::pin(
                crate::sync::device_join::prepare_device_registration_request(
                    &pending,
                    &storage.storage,
                    coordination,
                    None,
                    member,
                    *approval,
                ),
            )
            .await
            .expect("prepare exact registration request"),
        );
        let provisional = Box::new(
            Box::pin(
                crate::sync::device_join::accept_device_registration_request(
                    owner_db,
                    &storage.storage,
                    coordination,
                    &authorization,
                    owner,
                    *registration_request,
                ),
            )
            .await
            .expect("activate exact join attempt"),
        );
        let provider_ready = Box::new(
            Box::pin(crate::sync::device_join::publish_device_provider_challenge(
                owner_db,
                &storage.storage,
                None,
                (*provisional).clone(),
            ))
            .await
            .expect("publish same-principal provider readiness"),
        );
        let joining_db = match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => open_test_db(),
            crate::WritePolicy::Serial => open_serial_test_db(),
        };
        match owner_db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                storage
                    .open_into(&joining_db)
                    .await
                    .expect("open exact Merge Store for joining device");
            }
            crate::WritePolicy::Serial => {
                crate::sync::store_protocol_root::open_store(
                    &joining_db,
                    &storage.storage,
                    &storage.root,
                )
                .await
                .expect("open exact Serial Store for joining device");
            }
        }
        let (registration_visible, release_registration_create) =
            storage.home.pause_after_exact_create_call(1);
        let mut bootstrap = Box::pin(crate::sync::device_join::bootstrap_pending_device(
            &joining_db,
            &pending,
            &storage.storage,
            None,
            member,
            *provider_ready,
            T0,
        ));
        tokio::select! {
            () = registration_visible.notified() => {}
            result = &mut bootstrap => panic!(
                "bootstrap ended before reaching the registration create boundary: {result:?}"
            ),
        }
        let cancellation = Box::new(
            Box::pin(crate::sync::device_join::cancel_device_join(
                owner_db,
                &storage.storage,
                coordination,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel while registration create is in flight"),
        );
        let administrator = Box::new(
            Box::pin(crate::sync::device_join::close_device_provider_admission(
                owner_db,
                &storage.storage,
                None,
                owner,
                (*cancellation).clone(),
            ))
            .await
            .expect("close provider admission during late create"),
        );
        let joiner = Box::new(
            Box::pin(crate::sync::device_join::close_joining_device(
                &pending,
                &storage.storage,
                storage.home.as_ref(),
                &storage.root,
                member,
                (*cancellation).clone(),
            ))
            .await
            .expect("close joining device during late create"),
        );
        release_registration_create.notify_one();
        let bootstrap_result = bootstrap.await;
        assert!(
            bootstrap_result.is_err(),
            "a registration deleted by cancellation cannot complete bootstrap"
        );
        let receipt = Box::new(
            Box::pin(crate::sync::device_join::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
                coordination,
                storage.home.as_ref(),
                &authorization,
                owner,
                *cancellation,
                *administrator,
                *joiner,
            ))
            .await
            .expect("prepare cleanup after in-flight registration"),
        );
        Box::pin(crate::sync::device_join::activate_device_join_cleanup(
            owner_db,
            &storage.storage,
            coordination,
            &authorization,
            owner,
            provisional.publication_authorization.attempt.attempt_id,
            *receipt,
        ))
        .await
        .expect("activate cleanup after in-flight registration");
    })
}

async fn make_remote_intent_present(db: &Database, root_table: &str, root_id: &str) -> bool {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::make_remote_intent_exists(conn, &rt, &ri))
        .await
        .expect("make_remote intent lookup")
}

async fn pending_write_count(db: &Database) -> i64 {
    db.call(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM store_writes WHERE status = '\"pending\"'",
            [],
            |row| row.get(0),
        )
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("pending write count")
}

/// Queue a pending upload whose source file doesn't exist, so the cycle's drain
/// can't clear it — the entry stays pending, modeling a slow or stuck upload
/// while we assert the changeset/snapshot aren't held back by it.
async fn seed_pending_upload(db: &Database) {
    exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pending-root', 'Pending', NULL, 0, '0000000000001-0000-M', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('pending-blob', 'pending-root', 'cover', 1, '{}', \
                 '0000000000001-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"x"),
        ),
    )
    .await;
    let row = db
        .row_blob_ref("note_photos", "pending-blob")
        .await
        .expect("resolve exact pending blob row");
    db.call(move |conn| {
        Database::enqueue_upload_on(
            conn,
            "notes",
            "pending-root",
            &row,
            std::path::Path::new("/nonexistent/pending-blob"),
            false,
            T0,
        )
    })
    .await
    .expect("seed exact pending upload");
}

/// A pending cloud upload does not hold back a gated-true changeset: the gate
/// column decides per-row visibility, so a row that is shareable now reaches
/// peers without waiting for unrelated uploads to finish. The gate still cuts a
/// gated-false row, which is what withholds a not-yet-uploaded unit.
#[tokio::test]
async fn pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(
                run_pending_upload_does_not_hold_back_a_gated_true_changeset(),
            )
            .await
            .expect("pending-upload gate orchestration");
        })
        .await;
}

async fn run_pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let keypair = UserKeypair::generate();
    let blob_decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
    let db = open_test_db_with_blob(blob_decl.clone());
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([5u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));

    retain_store_packages_for_assertion(&db, &storage, b"existing-pending-upload-snapshot").await;
    let peer = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &keypair,
        &Hlc::new("M".to_string()),
        &pubkey_hex(&peer),
        None,
        crate::sync::membership::MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        "test-lib",
        "Test Store",
        &db,
    )
    .await
    .expect("invite exact pending-upload peer");
    let db_b = open_test_db_with_blob(blob_decl);
    activate_joined_test_device(&storage, &db, &db_b, &peer).await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact pending-upload device")
        .expect("exact pending-upload device exists");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("settle exact pending-upload peer activation");

    // A slow/stuck upload for some OTHER unit is pending the whole time.
    seed_pending_upload(&db).await;

    // One shareable note (its blobs are up → gate on) and one still-private note
    // (its blobs aren't up yet → gate off; the host hasn't flipped it).
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('priv', 'NotYet', NULL, 0, '0000000002000-0000-M', '2026-01-01')",
    )
    .await;

    // The changeset pushes despite the pending upload — no global deferral.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("publish gated-true write beside pending upload");

    // The activated peer pulls: it gets the shareable row, never the gated-false one.
    pull_into(&db_b, &storage, &ld).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'pub'").await,
        "Shareable",
        "the shareable note reaches the peer",
    );
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'priv'").await,
        "a gated-false row is still withheld — that is what holds a not-yet-uploaded unit",
    );
}

/// A gated-false row is withheld until its gate flips on, then it propagates: the
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (coven
/// flips the gate when a manage's blobs land; here the flip is written directly.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(run_gated_false_row_propagates_once_its_gate_flips())
                .await
                .expect("gate-flip propagation orchestration");
        })
        .await;
}

async fn run_gated_false_row_propagates_once_its_gate_flips() {
    let keypair = UserKeypair::generate();
    let blob_decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
    let db = open_test_db_with_blob(blob_decl.clone());
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([8u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let peer = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &keypair,
        &Hlc::new("M".to_string()),
        &pubkey_hex(&peer),
        None,
        crate::sync::membership::MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        "test-lib",
        "Test Store",
        &db,
    )
    .await
    .expect("invite exact gate-flip peer");
    let db_b = open_test_db_with_blob(blob_decl);
    activate_joined_test_device(&storage, &db, &db_b, &peer).await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact gate-flip device")
        .expect("exact gate-flip device exists");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("settle exact gate-flip peer activation");

    // A note whose blobs aren't up yet: gate off.
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("publish gated-false Store write");

    pull_into(&db_b, &storage, &ld).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-false row must not reach a peer",
    );

    // The blobs land; the host flips the gate on. The next cycle re-emits the
    // now-shareable row.
    host_exec(
        &db,
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("publish gate-flip Store write");

    // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
    // re-emits it at seq 1. Re-pull from empty positions to pick it up wherever it
    // landed.
    pull_into(&db_b, &storage, &ld).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "once its gate flips on, the row reaches the peer",
    );
}

/// The snapshot is the second propagation channel and runs the same row-level
/// gate (`delete_gated_false`), so a pending upload does not withhold it: the
/// snapshot carries the gated-true rows and excludes the gated-false ones, which
/// is the blob-before-row guarantee at snapshot granularity.
#[tokio::test]
async fn snapshot_is_not_withheld_by_pending_uploads() {
    let keypair = UserKeypair::generate();
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = cycle_test_store(&db, &keypair).await;
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [9u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    seed_pending_upload(&db).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        latest_store_snapshot_meta(&db).await.is_some(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

#[tokio::test]
async fn initial_snapshot_uploads_remote_root_host_blobs_before_publish() {
    let keypair = UserKeypair::generate();
    let db = open_test_db_schema(
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
            SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
                .carries_blob(BlobDecl::new(
                    "photos",
                    Provenance::HostProvided,
                    CacheFill::CacheEager,
                )),
        ],
        test_migrations(),
    );
    let storage = cycle_test_store(&db, &keypair).await;
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [11u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    // Remove the seed writes so the cycle takes the initial-snapshot path; the rows
    // still reach the cloud through the snapshot, which reads them from the db.
    let _ = capture_bytes(&db, &[]).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let stored = stored_blob_for_row(&db, "note_photos", "cover1")
        .await
        .expect("the snapshot activates its exact host blob binding");
    storage
        .storage
        .verify_blob_object(&stored)
        .await
        .expect("the blob referenced by the initial snapshot exists");
    assert!(
        latest_store_snapshot_meta(&db).await.is_some(),
        "the snapshot metadata publishes after its referenced blob exists",
    );
}

#[tokio::test]
async fn initial_snapshot_does_not_publish_when_host_blob_upload_fails() {
    let keypair = UserKeypair::generate();
    let tables = vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey).carries_blob(
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
        ),
    ];
    let db = open_test_db_schema(tables.clone(), test_migrations());
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([12u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));

    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    assert_eq!(pending_write_count(&db).await, 0);
    let membership = storage.open_into(&db).await.expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize MergeConcurrent test membership");

    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let cycle_storage = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let failed = run_cycle_in_task(
        Arc::clone(&cycle_storage),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect_err("snapshot publish should fail when a referenced blob cannot upload");

    assert_eq!(
        failed.to_string(),
        "publish Store snapshot: storage error: storage operation failed: unexpected blob create call 1",
        "cycle surfaces the exact blob upload failure",
    );
    assert!(
        failed.is_offline(),
        "snapshot host-blob provider transport is offline: {failed}",
    );
    let installed_bindings: i64 = db
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM row_blob_locators", [], |row| {
                row.get(0)
            })
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("count activated snapshot blob bindings");
    assert_eq!(installed_bindings, 0);
    let rejected = cycle_storage.rejected_blobs();
    assert_eq!(rejected.len(), 1);
    let pending = db
        .outbound_snapshot_publication()
        .await
        .expect("load retained snapshot publication")
        .expect("failed snapshot publication remains durable");
    assert_eq!(pending.blobs.len(), 1);
    assert_eq!(pending.blobs[0].bindings[0].blob(), &rejected[0]);
    assert!(pending.blobs[0]
        .spool_path
        .as_ref()
        .is_some_and(|path| path.is_file()));
    assert!(matches!(
        storage.storage.verify_blob_object(&rejected[0]).await,
        Err(StorageError::NotFound(_))
    ));
    assert!(
        latest_store_snapshot_meta(&db).await.is_none(),
        "snapshot metadata is not published when a referenced blob upload fails",
    );

    let pending_reference = pending.reference.clone();
    let pending_image = pending.meta.value.image.clone();
    let pending_spool = pending.blobs[0]
        .spool_path
        .clone()
        .expect("failed publication retains exact spool");
    exec(
        &db,
        "UPDATE note_photos
         SET size = 6,
             hash = 'b7cb0795b8e42b33917c4bc2007f7a3f49c6e2777927b004c1a2ff587fcb1a7f',
             _updated_at = '0000000002000-0000-M'
         WHERE id = 'cover1'",
    )
    .await;
    let retry_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read retry Store device")
        .expect("retry Store device exists");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair,
        Arc::clone(&hlc),
        ld.clone(),
        retry_device_id,
    )
    .await
    .expect("retry exact snapshot publication");

    let published = db
        .latest_local_store_snapshot()
        .await
        .expect("load published snapshot")
        .expect("retry publishes snapshot");
    assert_eq!(published.reference, pending_reference);
    assert_eq!(published.meta.image, pending_image);
    let live = db
        .row_blob_ref("note_photos", "cover1")
        .await
        .expect("live row remains pending its own remote locator");
    assert!(matches!(
        live.authority(),
        crate::blob::RowBlobAuthority::PendingRemote(crate::blob::locator::RemoteAudience::Store)
    ));
    assert!(live.stored().is_none());
    storage
        .storage
        .verify_blob_object(&rejected[0])
        .await
        .expect("retry publishes exact retained blob");
    assert!(db
        .outbound_snapshot_publication()
        .await
        .expect("load completed snapshot publication")
        .is_none());
    assert!(!pending_spool.exists());

    crate::blob::local_files::drop_blob(&ld, "photos", "cover1")
        .await
        .expect("remove source blob before restore");
    let (restore_temp, restore_dir) = temp_store_dir();
    let restore_path = restore_dir.db_path();
    let bootstrap = crate::sync::snapshot::bootstrap_from_snapshot(
        &storage.storage,
        None,
        "test-lib",
        storage.root.clone(),
        &crate::join_code::MembershipFloor::MergeConcurrent(membership.head_refs().to_vec()),
        db.schema_version(),
        &restore_path,
    )
    .await
    .expect("verify snapshot-only blob bootstrap");
    let restored = bootstrap
        .open_database(
            "test-lib",
            &restore_path,
            tables.clone(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            "restored-snapshot-device".to_string(),
            &test_migrations(),
        )
        .await
        .expect("install snapshot-only blob bootstrap");
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    assert_eq!(
        crate::sync::snapshot::reconcile_snapshot_blobs(
            &restored,
            &restore_path,
            &storage.storage,
            &restore_dir,
            &tables,
            &cancel_rx,
        )
        .await
        .expect("reconcile restored snapshot blob"),
        crate::sync::snapshot::SnapshotBlobReconcile::Complete,
    );
    assert_eq!(
        crate::blob::cache::read_staged(
            &restore_dir,
            &restored
                .row_blob_ref("note_photos", "cover1")
                .await
                .expect("load restored exact blob reference"),
        )
        .await
        .expect("read restored snapshot blob"),
        Some(b"cover".to_vec()),
    );
    drop(restore_temp);
}

#[tokio::test]
async fn initial_snapshot_removes_current_spool_when_blob_preparation_fails() {
    let keypair = UserKeypair::generate();
    let tables = vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey).carries_blob(
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
        ),
    ];
    let db = open_test_db_schema(tables, test_migrations());
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_tmp, store_dir) = temp_store_dir();
    let cipher = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([24u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&store_dir, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided snapshot blob");
    assert_eq!(pending_write_count(&db).await, 0);
    storage.open_into(&db).await.expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize exact snapshot membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_prepare(Arc::clone(
        &storage,
    )));

    let error = run_cycle_in_task(
        Arc::clone(&interceptor),
        db.clone(),
        cipher,
        keypair,
        hlc,
        store_dir.clone(),
        device_id,
    )
    .await
    .expect_err("snapshot blob preparation fails after sealing its spool");

    assert!(
        error.to_string().contains("unexpected blob prepare call 1"),
        "unexpected snapshot preparation failure: {error}",
    );
    assert_eq!(interceptor.blob_write_calls(), (1, 1, 0));
    assert!(db
        .outbound_snapshot_publication()
        .await
        .expect("load rejected snapshot publication")
        .is_none());
    assert!(db
        .snapshot_blob_spool_cleanup_paths()
        .await
        .expect("load rejected snapshot cleanup")
        .is_empty());
    let spool_dir = store_dir.storage_dir().join("outbound-blobs");
    let spool_count = match std::fs::read_dir(&spool_dir) {
        Ok(entries) => entries.count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!(
            "read snapshot spool directory {}: {error}",
            spool_dir.display()
        ),
    };
    assert_eq!(
        spool_count, 0,
        "failed preparation must not orphan its current snapshot spool",
    );
    assert_eq!(
        crate::blob::local_files::read(&store_dir, "photos", "cover1", 5)
            .await
            .expect("read retained snapshot source"),
        Some(b"cover".to_vec()),
    );
}

#[tokio::test]
async fn snapshot_blob_spool_cleanup_survives_database_restart() {
    let keypair = UserKeypair::generate();
    let tables = vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey).carries_blob(
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
        ),
    ];
    let database_dir = tempfile::tempdir().expect("snapshot cleanup database directory");
    let database_path = database_dir.path().join("store.db");
    let open = || {
        Database::open(
            &database_path,
            tables.clone(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "snapshot-cleanup-device".to_string(),
            &test_migrations(),
        )
        .expect("open snapshot cleanup database")
        .0
    };
    let db = open();
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_store_temp, store_dir) = temp_store_dir();
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&store_dir, "photos", "cover1", b"cover")
        .await
        .expect("store cleanup source blob");
    storage.open_into(&db).await.expect("open cleanup Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize cleanup membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read cleanup device")
        .expect("cleanup device exists");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(
        interceptor,
        db.clone(),
        Arc::new(RwLock::new(CloudCipher::Encrypted(
            EncryptionService::from_key([12u8; 32]),
        ))),
        keypair,
        Arc::new(Hlc::new("M".to_string())),
        store_dir,
        device_id,
    )
    .await
    .expect_err("retain cleanup spool after rejected blob create");
    let pending = db
        .outbound_snapshot_publication()
        .await
        .expect("load cleanup snapshot")
        .expect("cleanup snapshot remains pending");
    let spool = pending.blobs[0]
        .spool_path
        .clone()
        .expect("cleanup snapshot has spool");
    db.complete_snapshot_publication(pending.reference)
        .await
        .expect("atomically complete snapshot and own spool cleanup");
    assert_eq!(
        db.snapshot_blob_spool_cleanup_paths()
            .await
            .expect("load durable cleanup"),
        vec![spool.clone()]
    );
    drop(db);

    let reopened = open();
    assert!(crate::sync::store_snapshot::drain_outbound_store_snapshot(
        &storage.storage,
        &reopened,
    )
    .await
    .expect("drain cleanup after restart")
    .is_none());
    assert!(!spool.exists());
    assert!(reopened
        .snapshot_blob_spool_cleanup_paths()
        .await
        .expect("load completed cleanup")
        .is_empty());
}

#[tokio::test]
async fn initial_snapshot_coalesces_shared_exact_blob_across_row_bindings() {
    let keypair = UserKeypair::generate();
    let tables = vec![
        SyncedTable::new("assets", crate::sync::session::RowIdentity::SharedKey)
            .remote_root()
            .carries_blob(
                BlobDecl::new("assets", Provenance::HostProvided, CacheFill::CacheEager)
                    .with_id_column("blob_id"),
            ),
    ];
    let migrations = vec![crate::migration::Migration::sql(
        1,
        "shared snapshot blob",
        "CREATE TABLE assets (
            id TEXT PRIMARY KEY,
            blob_id TEXT NOT NULL,
            size INTEGER NOT NULL,
            hash TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )];
    let db = open_test_db_schema(tables, migrations);
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_temp, store_dir) = temp_store_dir();
    let hash = crate::blob::content_hash(b"shared");
    exec(
        &db,
        &format!(
            "INSERT INTO assets (id, blob_id, size, hash, _updated_at) VALUES
             ('row-a', 'blob-shared', 6, '{hash}', '0000000001000-0000-M'),
             ('row-b', 'blob-shared', 6, '{hash}', '0000000001000-0000-M')"
        ),
    )
    .await;
    crate::blob::local_files::store(&store_dir, "assets", "blob-shared", b"shared")
        .await
        .expect("store shared snapshot blob");
    storage
        .open_into(&db)
        .await
        .expect("open shared blob Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize shared blob membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read shared blob device")
        .expect("shared blob device exists");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create_on(
        Arc::clone(&storage),
        2,
    ));
    run_cycle_in_task(
        Arc::clone(&interceptor),
        db.clone(),
        Arc::new(RwLock::new(CloudCipher::Encrypted(
            EncryptionService::from_key([12u8; 32]),
        ))),
        keypair,
        Arc::new(Hlc::new("M".to_string())),
        store_dir,
        device_id,
    )
    .await
    .expect("publish coalesced shared snapshot blob");
    assert_eq!(interceptor.rejected_blobs().len(), 1);
    let (bindings, objects): (i64, i64) = db
        .call(|conn| {
            Ok((
                conn.query_row("SELECT COUNT(*) FROM row_blob_locators", [], |row| {
                    row.get(0)
                })?,
                conn.query_row("SELECT COUNT(*) FROM blob_locators", [], |row| row.get(0))?,
            ))
        })
        .await
        .expect("count coalesced snapshot graph");
    assert_eq!(bindings, 2);
    assert_eq!(objects, 1);
}

#[tokio::test]
async fn initial_snapshot_requires_existing_exact_user_blob_without_uploading_it() {
    let keypair = UserKeypair::generate();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (external_dir, store_dir) = temp_store_dir();
    let external_path = external_dir.path().join("audio1.flac");
    crate::local_blob::write_atomic(&external_path, b"AUDIO")
        .await
        .expect("write external snapshot blob");
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos
             (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('audio1', 'n1', 'audio', 5, '{}',
                     '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"AUDIO"),
        ),
    )
    .await;
    let external_path_for_registration = external_path
        .to_str()
        .expect("external snapshot path is UTF-8")
        .to_string();
    let plaintext_hash = crate::blob::content_hash(b"AUDIO");
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO local_blob_refs
             (table_name, row_id, column_name, row_stamp, namespace, blob_id,
              path, plaintext_size, plaintext_hash)
             VALUES ('note_photos', 'audio1', 'id', '0000000001000-0000-M',
                     'audio', 'audio1', ?1, 5, ?2)",
            rusqlite::params![external_path_for_registration, plaintext_hash],
        )?;
        Ok(())
    })
    .await
    .expect("register external snapshot blob");
    storage.open_into(&db).await.expect("open user blob Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize user blob membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read user blob device")
        .expect("user blob device exists");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let cipher = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([12u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));

    let error = run_cycle_in_task(
        Arc::clone(&interceptor),
        db.clone(),
        Arc::clone(&cipher),
        keypair.clone(),
        Arc::clone(&hlc),
        store_dir.clone(),
        device_id.clone(),
    )
    .await
    .expect_err("snapshot rejects a local-only user blob");
    assert_eq!(interceptor.blob_write_calls(), (0, 0, 0));
    assert!(
        error.to_string().contains(
            "snapshot UserProvided blob audio/audio1 has no existing exact remote binding"
        ),
        "unexpected snapshot rejection: {error}"
    );
    assert!(db
        .outbound_snapshot_publication()
        .await
        .expect("load rejected snapshot outbox")
        .is_none());
    assert!(db
        .snapshot_blob_spool_cleanup_paths()
        .await
        .expect("load rejected snapshot cleanup")
        .is_empty());
    assert!(db
        .latest_local_store_snapshot()
        .await
        .expect("load rejected published snapshot")
        .is_none());

    let stored = create_exact_blob(&storage, "audio", "audio1", b"AUDIO").await;
    assert!(!store_dir
        .outbound_blob_spool_path(stored.locator().locator_hash())
        .exists());
    let (registration_ref, registration) = db
        .activated_store_device_registration_records()
        .await
        .expect("load exact Store registrations")
        .into_iter()
        .find(|(_, registration)| registration.device_id.to_string() == device_id)
        .expect("local Store registration is activated");
    let record = crate::sync::remote_object::RemoteObjectRecord::snapshot_activated_blob(
        &stored,
        crate::sync::remote_object::SnapshotObjectOwner {
            activation: registration
                .store_snapshot_activation(&registration_ref)
                .expect("derive exact Store snapshot activation")
                .activation_id(),
            generation: 0,
        },
    )
    .expect("activate exact user blob for the initial snapshot");
    let object_id = record.object_id().to_string();
    let state = serde_json::to_string(&record).expect("serialize exact user blob state");
    let locator_hash = stored.locator().locator_hash().to_string();
    let audience = serde_json::to_string(&crate::sync::audience_package::PackageAudience::Store)
        .expect("serialize Store audience");
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![object_id, state],
        )?;
        conn.execute(
            "INSERT INTO blob_locators (locator_hash, remote_object_id) VALUES (?1, ?2)",
            rusqlite::params![locator_hash, record.object_id().to_string()],
        )?;
        conn.execute(
            "INSERT INTO row_blob_locators
             (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
             VALUES ('note_photos', 'audio1', 'id', '0000000001000-0000-M', ?1, ?2)",
            rusqlite::params![audience, record.object_id().to_string()],
        )?;
        Ok(())
    })
    .await
    .expect("install exact activated user blob binding");
    tokio::fs::remove_file(&external_path)
        .await
        .expect("remove external source before exact-binding retry");

    run_cycle_in_task(
        Arc::clone(&interceptor),
        db.clone(),
        cipher,
        keypair,
        hlc,
        store_dir,
        device_id,
    )
    .await
    .expect("publish snapshot from existing exact user blob");
    let (_, prepare_calls, create_calls) = interceptor.blob_write_calls();
    assert_eq!((prepare_calls, create_calls), (0, 0));
    assert!(interceptor.rejected_blobs().is_empty());
    assert!(db
        .outbound_snapshot_publication()
        .await
        .expect("load completed snapshot outbox")
        .is_none());
    assert!(db
        .latest_local_store_snapshot()
        .await
        .expect("load published user blob snapshot")
        .is_some());
}

// The drain's break-to-publish is now driven by a manage *completion* (coven flips
// the gate the moment the last blob lands), not by an observer signal. It is covered
// end-to-end in `blob::transition_tests` — `resume_drain_promptly` after a manage
// completes, with another root's blob left queued.

/// Founder-at-creation + owner anchoring (issue #102): the first cloud connect of
/// a created store writes the founder Owner entry and pins the owner; later
/// connects anchor the chain to that pinned owner; and a wiped or refounded chain
/// is refused as a takeover attempt.
#[tokio::test]
async fn ensure_owner_anchored_chain_founds_pins_and_refuses_tampering() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(&db, "test-store", owner.clone())
        .await
        .expect("create exact Store");

    ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &owner,
    )
    .await
    .expect("anchor the exact founder graph");
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in protocol_state",
    );
    let membership = crate::sync::pull::load_cycle_membership(&storage.storage, &db)
        .await
        .expect("load exact founder membership")
        .chain
        .expect("MergeConcurrent Store has membership");
    assert!(
        membership.is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &owner,
    )
    .await
    .expect("re-connect anchors to the pinned owner");
    let owner_before = db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let graph = db
        .local_store_founder_graph()
        .await
        .expect("read founder graph")
        .expect("founder graph exists");
    let crate::database::DurableFounderMembership::MergeConcurrent { head, .. } = graph.membership
    else {
        panic!("MergeConcurrent test Store has Serial founder membership");
    };
    storage
        .storage
        .delete_protocol_object(&head.object)
        .await
        .expect("delete exact founder head");
    assert!(
        ensure_owner_anchored_chain(
            &storage.storage,
            &db,
            &storage.root,
            storage.protocol_root(),
            &owner,
        )
        .await
        .is_err(),
        "a missing exact founder head is refused",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
}

#[tokio::test]
async fn owner_anchor_installs_founder_device_genesis() {
    let owner = UserKeypair::generate();
    let creator_db = open_test_db();
    let storage = TestStore::create(&creator_db, "test-store", owner.clone())
        .await
        .expect("create exact Store");
    let opened_db = open_test_db();
    let root =
        crate::sync::store_protocol_root::open_store(&opened_db, &storage.storage, &storage.root)
            .await
            .expect("open exact Store root");
    assert_eq!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read founder device genesis before anchoring"),
        None,
    );

    crate::sync::cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &opened_db,
        &storage.root,
        &root,
        &owner,
    )
    .await
    .expect("anchor exact Store founder");

    assert!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read anchored founder device genesis")
            .is_some(),
        "owner anchoring installs the founder state required by its first commit",
    );
}

/// Founding writes the cloud founder entry before pinning the owner, so a crash
/// between the two leaves a chain founded by our own key with no pin. The next
/// connect completes the pin (the founder is provably ours). A chain founded by a
/// DIFFERENT key with no pin is a first-connect takeover seed and is refused — the
/// branch that previously adopted any founder on trust.
#[tokio::test]
async fn exact_root_reanchors_own_founder_and_open_refuses_foreign_founder() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(&db, "test-store", owner.clone())
        .await
        .expect("create exact Store");
    db.delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove local owner pin");
    ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &owner,
    )
    .await
    .expect("re-anchor the exact founder");
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the exact founder restores its owner pin",
    );

    let attacker = UserKeypair::generate();
    let attacker_db = open_test_db();
    let seeded = TestStore::create(&attacker_db, "foreign-store", attacker)
        .await
        .expect("create foreign exact Store");
    let fresh_db = open_test_db();
    let foreign_root =
        crate::sync::store_protocol_root::open_store(&fresh_db, &seeded.storage, &seeded.root)
            .await
            .expect("open the pinned foreign Store root");
    assert!(
        ensure_owner_anchored_chain(
            &seeded.storage,
            &fresh_db,
            &seeded.root,
            &foreign_root,
            &owner,
        )
        .await
        .is_err(),
        "an exact root founded by another identity is refused",
    );
}

fn cloud_objects(home: &InMemoryCloudHome) -> BTreeMap<String, Vec<u8>> {
    home.keys()
        .into_iter()
        .map(|key| {
            let bytes = home.get(&key).expect("listed cloud object");
            (key, bytes)
        })
        .collect()
}

#[tokio::test]
async fn initializing_plaintext_storage_commits_and_pins_its_founder() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let db = open_test_db();
    let cipher = CloudCipher::Plaintext;
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );

    let components =
        cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore, None)
            .await
            .expect("initialize plaintext storage");

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let root = db
        .local_store_root_ref()
        .await
        .unwrap()
        .expect("initialization persists the exact Store root");
    let protocol_root =
        crate::sync::store_objects::load_store_protocol_root(components.storage().as_ref(), &root)
            .await
            .expect("open exact Store root")
            .value;
    let membership = crate::sync::pull::load_cycle_membership(components.storage().as_ref(), &db)
        .await
        .expect("load exact founder membership")
        .chain
        .expect("MergeConcurrent Store has membership");
    protocol_root
        .descriptor
        .validate_merge_founder_entry(
            membership
                .entries()
                .first()
                .expect("MergeConcurrent Store has a founder entry"),
        )
        .expect("membership begins with the descriptor's founder entry");
    let cursor_count = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key LIKE 'membership_head_cursor/%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn initializing_serial_storage_uses_only_the_root_authorization_state() {
    use crate::sync::membership::MemberRole;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let db = open_serial_test_db();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    )
    .with_test_serial_coordination(Arc::new(home.clone()));

    let components = Box::pin(cycle::init_sync_over_storage(
        &db,
        storage,
        cycle::StoreInitialization::CreateStore,
        None,
    ))
    .await
    .expect("initialize Serial storage");
    let (_temp, store_dir) = temp_store_dir();
    Box::pin(components.run_cycle(&SystemClock, None, &store_dir, None))
        .await
        .expect("run Serial cycle");

    assert!(home
        .keys()
        .iter()
        .all(|key| !key.starts_with("store-v1/membership/")));
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    assert_eq!(
        db.serial_membership_state()
            .await
            .unwrap()
            .expect("Serial root membership")
            .current_members(),
        vec![(owner_pk, MemberRole::Owner)],
    );
    assert_eq!(
        db.serial_key_generation().await.unwrap(),
        Some(crate::encryption::INITIAL_KEY_GENERATION),
    );
    let causal_floor_count = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key LIKE 'membership_head_cursor/%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(causal_floor_count, 0);
}

#[tokio::test]
async fn serial_cycle_uses_membership_materialized_by_its_pull_for_owner_only_work() {
    tokio::spawn(async {
        Box::pin(async {
            use std::sync::atomic::{AtomicUsize, Ordering};

            use crate::sync::membership::MemberRole;
            use crate::sync::storage::{
                CoordinationError, CoordinationStorage, CreateHeadError, ReplaceHeadError,
                VersionToken, VersionedObject,
            };
            use crate::sync::store_commit::StoreControl;

            struct HeadAdvancesAfterInitialAuthorization<'a> {
                inner: &'a dyn CoordinationStorage,
                initial: VersionedObject,
                reads: AtomicUsize,
            }

            #[async_trait::async_trait]
            impl CoordinationStorage for HeadAdvancesAfterInitialAuthorization<'_> {
                async fn provider_binding(
                    &self,
                ) -> Result<crate::sync::storage::ResolvedProviderBinding, CoordinationError>
                {
                    self.inner.provider_binding().await
                }

                async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError> {
                    if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Ok(self.initial.clone());
                    }
                    self.inner.read_head(key).await
                }

                async fn create_head(
                    &self,
                    key: &str,
                    bytes: &[u8],
                ) -> Result<VersionedObject, CreateHeadError> {
                    self.inner.create_head(key, bytes).await
                }

                async fn replace_head(
                    &self,
                    key: &str,
                    expected: &VersionToken,
                    bytes: &[u8],
                ) -> Result<VersionedObject, ReplaceHeadError> {
                    self.inner.replace_head(key, expected, bytes).await
                }

                async fn delete_head(&self, key: &str) -> Result<(), CoordinationError> {
                    self.inner.delete_head(key).await
                }
            }

            let store_id = "serial-post-pull-authorization";
            let founder = UserKeypair::generate();
            let successor = UserKeypair::generate();
            let local = open_serial_test_db();
            let store = Arc::new(
                TestStore::create(&local, store_id, founder.clone())
                    .await
                    .unwrap(),
            );
            let home = store.home.clone();
            let storage = &store.storage;
            let root = store.root.clone();
            let encryption = EncryptionService::from_key([42; 32]);
            let initial_head = storage
                .serial_coordination()
                .expect("Serial coordination")
                .read_head(crate::sync::store_commit::serial_head_key())
                .await
                .expect("read initial Serial head");
            let local_device_id = local
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await
                .unwrap()
                .expect("Serial founder device exists");
            let (_remote_dir, remote, remote_device_id) = Box::pin(recover_serial_owner_state(
                storage, &local, store_id, &root, &founder,
            ))
            .await;
            let coordination = storage.serial_coordination().unwrap();
            let (_, remote_registration, remote_registration_value, _) =
                crate::sync::store_outbound::load_local_store_authority(
                    &remote,
                    &remote_device_id,
                    &founder,
                )
                .await
                .expect("load recovered Owner device authority");
            let founder_provider_admin = &store.protocol_root.descriptor.founder_provider_admin;
            let provider_admin_plan = crate::sync::store_outbound::prepare_store_operation_commit(
                &remote,
                storage,
                crate::sync::store_outbound::StoreOperationPreparation::Serial { coordination },
                &remote_device_id,
                &founder,
            )
            .await
            .expect("prepare recovered provider administrator control");
            crate::sync::store_outbound::activate_store_operation_commit(
                &remote,
                storage,
                crate::sync::store_outbound::StoreOperationPublicationMode::Serial { coordination },
                provider_admin_plan,
                crate::sync::store_outbound::StoreOperationBatch::Control(
                    StoreControl::ProviderAdmin {
                        change: crate::sync::provider::ProviderAdminChange::Set {
                            administrator: remote_registration,
                            provider: remote_registration_value.provider,
                            access: founder_provider_admin.access.clone(),
                            capability: founder_provider_admin.capability.clone(),
                            grant_id:
                                crate::sync::provider::ProviderAdminGrantId::from_random_bytes(
                                    *crate::sync::store_commit::ObjectHash::digest(
                                        b"serial post-pull recovered provider administrator",
                                    )
                                    .as_bytes(),
                                ),
                            replaces: std::collections::BTreeSet::from([founder_provider_admin
                                .grant_id
                                .clone()]),
                        },
                    },
                ),
            )
            .await
            .expect("transfer provider administrator authority to recovered Owner device");
            crate::sync::membership_ops::invite_serial_member(
                storage,
                home.as_ref(),
                coordination,
                &remote_device_id,
                &founder,
                &Hlc::new("serial-successor-member".to_string()),
                &pubkey_hex(&successor),
                None,
                MemberRole::Member,
                &encryption,
                store_id,
                "Serial post-pull authorization Store",
                &remote,
            )
            .await
            .expect("add successor as a Serial Member");
            let successor_db = open_serial_test_db();
            install_active_device_fixture(
                store.as_ref(),
                &remote,
                &successor_db,
                &successor,
                "0000000002000-0000-successor",
            )
            .await
            .expect("activate successor device");
            let promotion_store = store.clone();
            let promotion_remote = remote.clone();
            let promotion_successor_db = successor_db.clone();
            let promotion_founder = founder.clone();
            let promotion_successor = successor.clone();
            let promotion_encryption = encryption.clone();
            tokio::spawn(async move {
                Box::pin(promote_active_member_fixture(
                    promotion_store.as_ref(),
                    &promotion_remote,
                    &promotion_successor_db,
                    &promotion_founder,
                    &promotion_successor,
                    &promotion_encryption,
                ))
                .await
            })
            .await
            .expect("successor promotion task")
            .expect("promote active successor Owner");
            let authorization =
                crate::sync::store_engine::serial::publication::current_serial_authorization(
                    &remote,
                    storage,
                    coordination,
                )
                .await
                .unwrap();
            let founder_pubkey = pubkey_hex(&founder);
            let founder_wrap = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
                storage,
                root.store_root_hash,
                &founder_pubkey,
                crate::sync::wrapped_store_key::WrappedStoreKey::signed(
                    &root.store_root_id.to_string(),
                    &founder_pubkey,
                    1,
                    b"cycle Serial founder role wrap".to_vec(),
                    &founder,
                ),
            )
            .await
            .unwrap();
            let demote_founder = authorization
                .membership
                .signed_set_member_with_wrapped_key(
                    &founder,
                    founder_pubkey,
                    None,
                    MemberRole::Follower,
                    founder_wrap.reference.clone(),
                    "0000000000003-0000-founder".to_string(),
                )
                .unwrap();
            crate::sync::store_outbound::activate_test_serial_control_candidate(
                &remote,
                storage,
                coordination,
                &remote_device_id,
                &founder,
                StoreControl::SerialMembership {
                    entry: demote_founder,
                },
                vec![founder_wrap],
            )
            .await
            .unwrap();

            let snapshot_before = local
                .latest_local_store_snapshot()
                .await
                .unwrap()
                .expect("Serial bootstrap snapshot was published locally")
                .reference;
            drop(remote);
            drop(_remote_dir);
            let store = store.clone();
            tokio::spawn(async move {
                Box::pin(async move {
                    let storage = &store.storage;
                    assert_eq!(local.local_store_root_ref().await.unwrap(), Some(root));
                    let coordination = storage.serial_coordination().unwrap();
                    let delayed = HeadAdvancesAfterInitialAuthorization {
                        inner: coordination,
                        initial: initial_head,
                        reads: AtomicUsize::new(0),
                    };
                    let (_temp, store_dir) = temp_store_dir();
                    let cipher = storage.cipher_state().clone();
                    let pending_rotation = storage.shared_pending_rotation();
                    cycle::run_single_sync_cycle_with_coordination(
                        storage,
                        Some(&delayed),
                        &local_device_id,
                        &Hlc::new(local_device_id.clone()),
                        &SystemClock,
                        &local,
                        cipher.as_ref(),
                        pending_rotation.as_ref(),
                        &founder,
                        None,
                        None,
                        &store_dir,
                        Some(home.as_ref()),
                        None,
                    )
                    .await
                    .expect("run cycle across a newly visible Serial control chain");

                    let mut expected_members = vec![
                        (pubkey_hex(&founder), MemberRole::Follower),
                        (pubkey_hex(&successor), MemberRole::Owner),
                    ];
                    expected_members.sort_by(|left, right| left.0.cmp(&right.0));
                    assert_eq!(
                        local
                            .serial_membership_state()
                            .await
                            .unwrap()
                            .unwrap()
                            .current_members(),
                        expected_members,
                    );
                    assert_eq!(
                        local
                            .latest_local_store_snapshot()
                            .await
                            .unwrap()
                            .expect("Serial bootstrap snapshot remains published")
                            .reference,
                        snapshot_before,
                    );
                })
                .await;
            })
            .await
            .expect("Serial post-pull cycle task");
        })
        .await;
    })
    .await
    .expect("Serial post-pull cycle orchestration task");
}

#[tokio::test]
async fn serial_cycle_marks_a_stale_provisional_branch_before_materializing_remote_commits() {
    tokio::spawn(async {
        let store_id = "serial-conflict-before-pull";
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = cycle_cloud_storage(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            store_id,
            owner.clone(),
        )
        .with_test_serial_coordination(Arc::new(home.clone()));
        let local = open_serial_test_db();
        let root = create_exact_test_store(&local, &storage, store_id, &owner)
            .await
            .expect("create exact Serial Store");
        let local_device_id = local
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("Serial founder device exists");
        let (_remote_dir, remote, remote_device_id) = Box::pin(recover_serial_owner_state(
            &storage, &local, store_id, &root, &owner,
        ))
        .await;
        host_exec(
            &remote,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('remote-row', 'remote', NULL, 1, '0000000001000-0000-remote', '2026-01-01')",
        )
        .await;
        let (_remote_temp, remote_store_dir) = temp_store_dir();
        assert!(
            crate::sync::store_engine::serial::publication::prepare_serial_store_branch(
                &remote,
                &storage,
                storage.serial_coordination().unwrap(),
                &remote_device_id,
                &owner,
                &remote_store_dir
            )
            .await
            .unwrap()
        );
        assert_eq!(
            crate::sync::store_engine::serial::publication::drain_store_writes(
                &remote,
                &storage,
                storage.serial_coordination().unwrap(),
            )
            .await
            .unwrap(),
            1
        );

        assert_eq!(
            local.local_store_root_ref().await.unwrap(),
            Some(root.clone())
        );
        host_exec(
            &local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('local-row', 'local', NULL, 1, '0000000001000-0000-local', '2026-01-01')",
        )
        .await;
        let local_write = local.pending_writes().await.unwrap().remove(0).write_id;
        let (_local_temp, local_store_dir) = temp_store_dir();
        cycle::run_single_sync_cycle_with_coordination(
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id,
            &Hlc::new(local_device_id.clone()),
            &SystemClock,
            &local,
            storage.cipher_state().as_ref(),
            storage.shared_pending_rotation().as_ref(),
            &owner,
            None,
            None,
            &local_store_dir,
            Some(&home),
            None,
        )
        .await
        .expect("record the stale provisional branch without applying its successor");

        assert!(matches!(
            local.write_status(&local_write).await.unwrap(),
            crate::WriteStatus::Conflict(_)
        ));
        assert_eq!(
            local
                .exact_materialized_ref(crate::sync::store_commit::SERIAL_STREAM_ID, 2)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            query_text(
                &local,
                "SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'remote-row'"
            )
            .await,
            "0"
        );
        assert_eq!(
            query_text(&local, "SELECT title FROM notes WHERE id = 'local-row'").await,
            "local"
        );
    })
    .await
    .expect("Serial stale provisional branch cycle task");
}

#[tokio::test]
async fn serial_cycle_publishes_a_suffix_rebased_by_its_initial_drain() {
    tokio::spawn(async {
        let store_id = "serial-cycle-rebased-suffix";
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = cycle_cloud_storage(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            store_id,
            owner.clone(),
        )
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_serial_test_db();
        let root = create_exact_test_store(&db, &storage, store_id, &owner)
            .await
            .expect("create exact Serial Store");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("Serial founder device exists");
        crate::sync::store_snapshot::push_store_snapshot(
            &storage,
            root.store_root_hash,
            crate::sync::snapshot::CreatedSnapshot {
                db_image: b"existing-snapshot".to_vec(),
                blobs: Vec::new(),
            },
            crate::sync::store_commit::CommitFrontier::Serial(None),
            db.schema_version(),
            &owner,
            T0.to_string(),
            None,
            &db,
        )
        .await
        .expect("publish exact Serial snapshot fixture");
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-first', 'first', NULL, 1, '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
        let first = db.pending_writes().await.unwrap().pop().unwrap().write_id;
        let (_temp, store_dir) = temp_store_dir();
        assert!(
            crate::sync::store_engine::serial::publication::prepare_serial_store_branch(
                &db,
                &storage,
                storage.serial_coordination().unwrap(),
                &device_id,
                &owner,
                &store_dir
            )
            .await
            .unwrap()
        );
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-suffix', 'suffix', NULL, 1, '0000000001001-0000-owner', '2026-01-01')",
        )
        .await;
        let suffix = db.pending_writes().await.unwrap().pop().unwrap().write_id;

        cycle::run_single_sync_cycle_with_coordination(
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &device_id,
            &Hlc::new(device_id.clone()),
            &SystemClock,
            &db,
            storage.cipher_state().as_ref(),
            storage.shared_pending_rotation().as_ref(),
            &owner,
            None,
            None,
            &store_dir,
            Some(&home),
            None,
        )
        .await
        .expect("run cycle after a write joined the publishing branch");

        let first_status = db.write_status(&first).await.unwrap();
        assert!(
            matches!(
                &first_status,
                crate::WriteStatus::Published(position)
                    if matches!(
                        position.as_ref(),
                        crate::PublishedPosition::Serial { commit }
                            if commit.coord.sequence() == 1
                    )
            ),
            "unexpected first status: {first_status:?}"
        );
        let suffix_status = db.write_status(&suffix).await.unwrap();
        assert!(
            matches!(
                &suffix_status,
                crate::WriteStatus::Published(position)
                    if matches!(
                        position.as_ref(),
                        crate::PublishedPosition::Serial { commit }
                            if commit.coord.sequence() == 2
                    )
            ),
            "unexpected suffix status: {suffix_status:?}"
        );
    })
    .await
    .expect("Serial suffix cycle orchestration task");
}

#[tokio::test]
async fn initialization_refuses_a_founder_entry_without_its_store_protocol_root() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let seeded_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let seed_db = open_test_db();
    let root = create_exact_test_store(&seed_db, &seeded_storage, "test-lib", &owner)
        .await
        .expect("create exact Store fixture");
    seeded_storage
        .delete_protocol_object(&root.object)
        .await
        .expect("remove exact Store root while retaining its founder graph");

    let db = open_test_db();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );

    let error = match cycle::init_sync_over_storage(
        &db,
        storage,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("an exact founder graph without its Store root must fail loud"),
    };
    assert!(
        matches!(error, cycle::InitSyncError::StoreProtocolRoot(_)),
        "{error}"
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_refuses_a_foreign_founder_without_store_protocol_root() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    let attacker_db = open_test_db();
    let root = create_exact_test_store(&attacker_db, &attacker_storage, "test-lib", &attacker)
        .await
        .expect("create foreign exact Store fixture");
    attacker_storage
        .delete_protocol_object(&root.object)
        .await
        .expect("remove foreign exact Store root");

    let owner = UserKeypair::generate();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    let db = open_test_db();
    let error = match cycle::init_sync_over_storage(
        &db,
        storage,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("a foreign founder graph without its exact root must fail loud"),
    };
    assert!(
        matches!(error, cycle::InitSyncError::StoreProtocolRoot(_)),
        "{error}"
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_pins_a_committed_self_founder_without_cloud_rewrite() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let seeded_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let seed_db = open_test_db();
    let root = create_exact_test_store(&seed_db, &seeded_storage, "test-lib", &owner)
        .await
        .expect("create committed exact Store fixture");
    let cloud_before = cloud_objects(&home);

    let db = open_test_db();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    cycle::init_sync_over_storage(
        &db,
        storage,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    .expect("accept the identity's committed founder");

    assert_eq!(cloud_objects(&home), cloud_before);
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let cursor_count = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key LIKE 'membership_head_cursor/%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn plaintext_initialization_refuses_a_committed_foreign_founder_without_mutation() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    let attacker_db = open_test_db();
    let root = create_exact_test_store(&attacker_db, &attacker_storage, "test-lib", &attacker)
        .await
        .expect("create committed foreign Store");
    let cloud_before = cloud_objects(&home);

    let victim = UserKeypair::generate();
    let db = open_test_db();
    let cipher = CloudCipher::Plaintext;
    let victim_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        victim.clone(),
    );

    assert!(
        cycle::init_sync_over_storage(
            &db,
            victim_storage,
            cycle::StoreInitialization::OpenStore {
                expected_store_root: root,
            },
            None,
        )
        .await
        .is_err(),
        "a committed foreign founder prevents initialization",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    let watermark_count = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key LIKE 'membership_head_seq/%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(watermark_count, 0);
    let cloud_after = cloud_objects(&home);
    assert_eq!(cloud_after, cloud_before, "cloud objects are unchanged");
}

#[tokio::test]
async fn initialization_rejects_incoherent_cipher_and_blob_path_scheme() {
    for (cipher, blob_paths) in [
        (CloudCipher::Plaintext, BlobPathScheme::Hashed),
        (
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Plain,
        ),
    ] {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let db = open_test_db();
        let storage = cycle_cloud_storage(
            Arc::new(home.clone()),
            cipher.clone(),
            blob_paths,
            "test-lib",
            owner.clone(),
        );
        db.set_protocol_state(
            crate::sync::cloud_storage::ROTATION_GATE_STATE_KEY,
            "invalid rotation gate",
        )
        .await
        .unwrap();
        let pending_rotation = storage.shared_pending_rotation();

        assert!(
            cycle::init_sync_over_storage(
                &db,
                storage,
                cycle::StoreInitialization::CreateStore,
                None,
            )
            .await
            .is_err(),
            "incoherent at-rest representation must be refused",
        );
        assert!(home.is_empty(), "the cloud is unchanged");
        assert_eq!(
            db.get_protocol_state("owner_pubkey").await.unwrap(),
            None,
            "the local owner is not pinned",
        );
        assert_eq!(
            pending_rotation.pending_generation(),
            None,
            "the in-memory pending-rotation marker is not restored",
        );
        assert_eq!(
            db.get_protocol_state(crate::sync::cloud_storage::ROTATION_GATE_STATE_KEY)
                .await
                .unwrap(),
            Some("invalid rotation gate".to_string()),
            "the durable pending-rotation state is unchanged",
        );
    }
}

// ---- Host writes journal; applies never do ----

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::sync::storage::StorageError;

/// A [`SyncStorage`] that injects a host write at a cycle `await` point — the
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
    interception: CycleStorageInterception,
    protocol_read_calls: AtomicUsize,
}

enum CycleStorageInterception {
    PassThrough,
    RejectAckCreate,
    InjectHostWrite {
        db: Database,
        write_sql: String,
        fired: AtomicBool,
    },
    RejectBlobCreate {
        reject_create_call: Option<usize>,
        reject_prepare_call: Option<usize>,
        allocate_calls: std::sync::atomic::AtomicUsize,
        prepare_calls: std::sync::atomic::AtomicUsize,
        create_calls: std::sync::atomic::AtomicUsize,
        attempted: std::sync::Mutex<Vec<crate::blob::locator::StoredBlobRef>>,
    },
}

impl CycleStorageInterceptor {
    fn pass_through(inner: Arc<TestStore>) -> Self {
        Self {
            inner,
            interception: CycleStorageInterception::PassThrough,
            protocol_read_calls: AtomicUsize::new(0),
        }
    }

    fn reject_ack_create(inner: Arc<TestStore>) -> Self {
        Self {
            inner,
            interception: CycleStorageInterception::RejectAckCreate,
            protocol_read_calls: AtomicUsize::new(0),
        }
    }

    fn inject_host_write(inner: TestStore, db: Database, write_sql: &str) -> Self {
        Self {
            inner: Arc::new(inner),
            interception: CycleStorageInterception::InjectHostWrite {
                db,
                write_sql: write_sql.to_string(),
                fired: AtomicBool::new(false),
            },
            protocol_read_calls: AtomicUsize::new(0),
        }
    }

    fn reject_blob_create(inner: Arc<TestStore>) -> Self {
        Self::reject_blob_create_on(inner, 1)
    }

    fn reject_blob_create_on(inner: Arc<TestStore>, reject_call: usize) -> Self {
        assert!(reject_call > 0, "blob create call numbers are 1-based");
        Self {
            inner,
            interception: CycleStorageInterception::RejectBlobCreate {
                reject_create_call: Some(reject_call),
                reject_prepare_call: None,
                allocate_calls: std::sync::atomic::AtomicUsize::new(0),
                prepare_calls: std::sync::atomic::AtomicUsize::new(0),
                create_calls: std::sync::atomic::AtomicUsize::new(0),
                attempted: std::sync::Mutex::new(Vec::new()),
            },
            protocol_read_calls: AtomicUsize::new(0),
        }
    }

    fn reject_blob_prepare(inner: Arc<TestStore>) -> Self {
        Self {
            inner,
            interception: CycleStorageInterception::RejectBlobCreate {
                reject_create_call: None,
                reject_prepare_call: Some(1),
                allocate_calls: std::sync::atomic::AtomicUsize::new(0),
                prepare_calls: std::sync::atomic::AtomicUsize::new(0),
                create_calls: std::sync::atomic::AtomicUsize::new(0),
                attempted: std::sync::Mutex::new(Vec::new()),
            },
            protocol_read_calls: AtomicUsize::new(0),
        }
    }

    fn rejected_blobs(&self) -> Vec<crate::blob::locator::StoredBlobRef> {
        match &self.interception {
            CycleStorageInterception::RejectBlobCreate { attempted, .. } => attempted
                .lock()
                .expect("attempted blob record lock")
                .clone(),
            CycleStorageInterception::PassThrough
            | CycleStorageInterception::RejectAckCreate
            | CycleStorageInterception::InjectHostWrite { .. } => {
                panic!("storage interception does not reject blob creates")
            }
        }
    }

    fn blob_write_calls(&self) -> (usize, usize, usize) {
        match &self.interception {
            CycleStorageInterception::RejectBlobCreate {
                allocate_calls,
                prepare_calls,
                create_calls,
                ..
            } => (
                allocate_calls.load(Ordering::SeqCst),
                prepare_calls.load(Ordering::SeqCst),
                create_calls.load(Ordering::SeqCst),
            ),
            CycleStorageInterception::PassThrough
            | CycleStorageInterception::RejectAckCreate
            | CycleStorageInterception::InjectHostWrite { .. } => {
                panic!("storage interception does not record blob writes")
            }
        }
    }

    fn protocol_read_calls(&self) -> usize {
        self.protocol_read_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SyncStorage for CycleStorageInterceptor {
    fn store_blob_protection(
        &self,
    ) -> Result<crate::sync::storage::BlobSpoolProtection, StorageError> {
        self.inner.storage.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, StorageError> {
        self.inner.storage.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::storage::cloud::ObjectSlot, StorageError> {
        self.inner
            .storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::sync::storage::PreparedExactObject, StorageError> {
        self.inner
            .storage
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::sync::storage::PreparedExactObject,
    ) -> Result<(), StorageError> {
        if matches!(
            &self.interception,
            CycleStorageInterception::RejectAckCreate
        ) && prepared
            .reference()
            .slot()
            .logical_key()
            .starts_with("store-v1/acks/")
        {
            return Err(StorageError::Storage(
                "unexpected Store acknowledgement create".to_string(),
            ));
        }
        self.inner.storage.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        object: &crate::sync::storage::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        self.protocol_read_calls.fetch_add(1, Ordering::SeqCst);
        if semantic_prefix.starts_with("store-v1/candidates/")
            && semantic_prefix.contains("/packages/")
        {
            if let CycleStorageInterception::InjectHostWrite {
                db,
                write_sql,
                fired,
            } = &self.interception
            {
                if !fired.swap(true, Ordering::SeqCst) {
                    host_exec(db, write_sql).await;
                }
            }
        }
        self.inner
            .storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::sync::storage::ExactObjectRef), StorageError> {
        self.protocol_read_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .storage
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::sync::storage::PreparedExactObject), StorageError> {
        self.protocol_read_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .storage
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<(), StorageError> {
        self.inner.storage.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
    ) -> Result<crate::storage::cloud::ObjectSlot, StorageError> {
        if let CycleStorageInterception::RejectBlobCreate { allocate_calls, .. } =
            &self.interception
        {
            allocate_calls.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .storage
            .allocate_blob_slot(locator, authority)
            .await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        protection: crate::sync::storage::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.inner
            .storage
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        slot: crate::storage::cloud::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, StorageError> {
        if let CycleStorageInterception::RejectBlobCreate {
            reject_prepare_call,
            prepare_calls,
            ..
        } = &self.interception
        {
            let call = prepare_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if *reject_prepare_call == Some(call) {
                return Err(StorageError::Storage(format!(
                    "unexpected blob prepare call {call}"
                )));
            }
        }
        self.inner
            .storage
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError> {
        if let CycleStorageInterception::RejectBlobCreate {
            reject_create_call,
            create_calls,
            attempted,
            ..
        } = &self.interception
        {
            attempted
                .lock()
                .expect("attempted blob record lock")
                .push(blob.clone());
            let call = create_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if *reject_create_call == Some(call) {
                return Err(StorageError::Storage(format!(
                    "unexpected blob create call {call}"
                )));
            }
        }
        self.inner
            .storage
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        self.inner.storage.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError> {
        self.inner
            .storage
            .stage_exact_blob_download(blob, dest)
            .await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::sync::storage::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError> {
        self.inner
            .storage
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        self.inner.storage.delete_blob_object(blob).await
    }
}

/// A host write made WHILE a cycle is in its push/pull network phase
/// must land in the device's NEXT outgoing changeset. It is recorded by the same
/// durable journal path as any other host write.
///
/// Setup: a peer "A" has a changeset in shared storage. Device "M" runs a cycle
/// that pulls it; the storage wrapper injects a host INSERT into M at the
/// immutable package-read await inside the pull. We then assert the
/// injected row is (a) present locally on M and (b) carried in M's next outgoing
/// changeset — proven by pulling that changeset into a fresh peer.
///
/// Mutation proof: route the injected write through raw `Database::call` instead
/// of the host journal. The row commits locally, but it is absent from M's next
/// changeset and assertion (b) fails.
#[tokio::test]
async fn host_write_during_pull_lands_in_next_outgoing_changeset() {
    let tasks = tokio::task::LocalSet::new();
    tasks
        .run_until(async {
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                let hlc = Hlc::new("M".to_string());
                let (_tmp, ld) = temp_store_dir();
                let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
                    [4u8; 32],
                )));

                // A peer A has published one changeset (an insert of note 'a1') to shared
                // storage, so M's cycle has something to fetch — the await we inject at.
                let producer_db = open_test_db();
                let inner = cycle_test_store(&producer_db, &keypair).await;
                let a_src = open_test_db();
                let a_cs = capture_bytes(
                    &a_src,
                    &[
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
                    ],
                )
                .await;
                // M's database. The injector runs this INSERT into M at the package-read
                // await, mid-pull.
                let db_m = open_test_db();
                activate_joined_test_device(&inner, &producer_db, &db_m, &keypair).await;
                retain_store_packages_for_assertion(&db_m, &inner, b"existing-host-write-snapshot")
                    .await;
                let peer_sequence = producer_db
                    .latest_local_store_position()
                    .await
                    .expect("read producer Store position after activating M")
                    .expect("M's activation advances the producer Store stream")
                    .coord
                    .sequence()
                    .checked_add(1)
                    .expect("producer Store sequence remains representable");
                inner
                    .publish_changeset("A", peer_sequence, &a_cs, SCHEMA_VERSION)
                    .await
                    .expect("publish exact peer changeset after activating M");
                let storage = CycleStorageInterceptor::inject_host_write(
                    inner,
                    db_m.clone(),
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('m_mid', 'WrittenMidCycle', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
                );
                let db_c = open_test_db();
                activate_joined_test_device(&storage.inner, &producer_db, &db_c, &keypair).await;

                drop(a_src);
                drop(producer_db);
                tokio::spawn(async move {
                    // Cycle 1: M pulls A's changeset; the host write fires mid-pull.
                    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

                    // (a) The injected row is present locally on M.
                    assert!(
                        row_exists(&db_m, "SELECT 1 FROM notes WHERE id = 'm_mid'").await,
                        "the package read injects the host write into M",
                    );
                    assert_eq!(
                        query_text(&db_m, "SELECT title FROM notes WHERE id = 'm_mid'").await,
                        "WrittenMidCycle",
                        "the mid-cycle host write committed to M's local db",
                    );

                    // (b) The injected row has its own pending write. Cycle 2 publishes it. A fresh
                    // peer C pulls M's output and must receive 'm_mid'.
                    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

                    pull_into(&db_c, &storage.inner, &ld).await;
                    assert!(
                        row_exists(&db_c, "SELECT 1 FROM notes WHERE id = 'm_mid'").await,
                        "M's next Store commit carries the injected host write",
                    );
                    assert_eq!(
                        query_text(&db_c, "SELECT title FROM notes WHERE id = 'm_mid'").await,
                        "WrittenMidCycle",
                        "the mid-cycle host write reached a peer via M's next outgoing changeset",
                    );
                })
                .await
                .expect("mid-pull host write cycle task");
            })
            .await
            .expect("mid-pull host write setup task");
        })
        .await;
}

/// The other half of the write-ledger invariant: an applied row must not echo.
/// After M applies a peer's changeset, M's own next Store commit must not carry the
/// applied rows because remote apply does not use the host transaction path.
///
/// Mutation proof: route the apply through `run_internal_store_write_transaction_on`.
/// The applied rows then enter M's write ledger and republish, so device C receives
/// note 'a1' attributed to M and the assertion fails.
#[tokio::test]
async fn applied_rows_do_not_echo_into_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([6u8; 32]),
    )));

    // Peer A publishes a changeset; M pulls and applies it in cycle 1.
    let producer_db = open_test_db();
    let storage = Arc::new(cycle_test_store(&producer_db, &keypair).await);
    let db_m = open_test_db();
    activate_joined_test_device(&storage, &producer_db, &db_m, &keypair).await;
    let device_id = db_m
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read M Store device")
        .expect("M Store device exists");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    let peer_sequence = producer_db
        .latest_local_store_position()
        .await
        .expect("read producer Store position after activating M")
        .expect("M's activation advances the producer Store stream")
        .coord
        .sequence()
        .checked_add(1)
        .expect("producer Store sequence remains representable");
    storage
        .publish_changeset("A", peer_sequence, &a_cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");

    run_cycle_in_task(
        Arc::clone(&cycle_storage),
        db_m.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("M's pull cycle succeeds");
    assert_eq!(
        query_text(&db_m, "SELECT title FROM notes WHERE id = 'a1'").await,
        "FromA",
        "M applied A's changeset",
    );

    // Cycle 2 has no host write. The applied row must not create a local data
    // commit because apply bypasses the host write ledger.
    let before = db_m
        .latest_local_store_position()
        .await
        .expect("read local Store position before the empty cycle");
    run_cycle_in_task(
        cycle_storage,
        db_m.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("M's empty cycle succeeds");
    let after = db_m
        .latest_local_store_position()
        .await
        .expect("read local Store position after the empty cycle")
        .expect("the empty cycle publishes its acknowledgement");
    let (_, registration) = db_m
        .local_blob_write_authority()
        .await
        .expect("load local Store registration");
    let commit = crate::sync::store_objects::load_commit_ref(
        &storage.storage,
        storage.root.store_root_hash,
        &after,
        &registration,
    )
    .await
    .expect("load empty-cycle acknowledgement commit")
    .value;
    assert_eq!(
        commit.order.predecessor(),
        before.as_ref(),
        "the acknowledgement directly extends the previous local position",
    );
    assert!(commit.acknowledgement().is_some());
    assert!(commit.store_package().is_none());
}

#[tokio::test]
async fn captured_changeset_retries_after_host_provided_blob_upload_failure() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([8u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    storage.open_into(&db).await.expect("open exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let failed = match run_cycle_in_task(
        Arc::clone(&reject_blob_create),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    {
        Ok(_) => panic!("blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.to_string().contains("unexpected blob create call 1"),
        "cycle surfaces the blob upload failure: {failed}"
    );
    let pending = db
        .pending_writes()
        .await
        .expect("read retryable Store writes");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, crate::WriteStatus::Publishing);
    let rejected_blobs = reject_blob_create.rejected_blobs();
    assert_eq!(rejected_blobs.len(), 1);
    let prepared_blob = rejected_blobs[0].clone();
    let prepared = db
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write after blob failure")
        .expect("provider failure retains the exact prepared publication");
    assert_eq!(prepared.audiences.blobs.len(), 1);
    assert_eq!(prepared.audiences.blobs[0].blob(), &prepared_blob);
    assert!(
        storage
            .storage
            .verify_blob_object(&prepared_blob)
            .await
            .is_err(),
        "the failed blob upload did not publish the blob"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("host blob retry cycle succeeds");
    assert_eq!(
        pending_write_count(&db).await,
        0,
        "the pending writes clear once the retry publishes"
    );
    let activated_blob = stored_blob_for_row(&db, "note_photos", "hponly")
        .await
        .expect("retry activates the exact row blob binding");
    assert_eq!(activated_blob, prepared_blob);
    storage
        .storage
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry uploads and reads back the exact host-provided blob");
}

#[tokio::test]
async fn each_host_write_publishes_the_blob_facts_from_its_own_commit() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([24u8; 32]),
    )));
    let blob_decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let db = open_test_db_with_blob(blob_decl.clone());
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    retain_store_packages_for_assertion(&db, &storage, b"each-host-write-blob-facts").await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read package writer device")
        .expect("package writer device exists");
    host_exec(
        &db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01'); \
         INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('photo', 'n1', 'cover', 5, '{}', 'blob-a', \
                 '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"first"),
        ),
    )
    .await;
    host_exec(
        &db,
        &format!(
            "UPDATE note_photos \
             SET blob_id = 'blob-b', size = 6, hash = '{}', \
                 _updated_at = '0000000002000-0000-M' \
             WHERE id = 'photo'",
            crate::blob::content_hash(b"second"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "blob-a", b"first")
        .await
        .expect("store first write's blob");
    crate::blob::local_files::store(&ld, "photos", "blob-b", b"second")
        .await
        .expect("store second write's blob");

    let error = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::reject_ack_create(Arc::clone(
            &storage,
        ))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect_err("acknowledgement create stops the cycle before package reclamation");
    assert!(
        error
            .to_string()
            .contains("unexpected Store acknowledgement create"),
        "unexpected post-package failure: {error}"
    );
    assert!(latest_store_snapshot_meta(&db).await.is_some());

    let stream_id = local_store_stream_id(&db).await;
    let mut published_blob_ids = Vec::new();
    for seq in [1, 2] {
        let (commit_ref, commit) =
            load_exact_materialized_commit(&db, &storage.storage, &stream_id, seq)
                .await
                .expect("load exact materialized commit")
                .expect("write has a commit");
        let package = crate::sync::store_objects::load_store_package(
            &storage.storage,
            &commit_ref,
            &commit.value,
        )
        .await
        .expect("load exact Store package")
        .expect("commit has a package");
        let package = crate::sync::audience_package::AudiencePackage::parse(&package.value)
            .expect("parse exact audience package");
        for binding in package.blob_bindings() {
            storage
                .storage
                .verify_blob_object(binding.blob())
                .await
                .expect("committed blob object exists exactly");
        }
        published_blob_ids.push(
            package
                .blob_bindings()
                .iter()
                .map(|binding| binding.blob().locator().blob_id().to_string())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        published_blob_ids,
        vec![vec!["blob-a".to_string()], vec!["blob-b".to_string()]],
    );
}

/// A committed store-key rotation this device has not adopted pauses sealing
/// without taking down the cycle: a pending write that references a
/// host-provided blob stays queued while `rotation_pending` is set. A cycle after
/// adoption publishes the write and uploads its blob under the adopted key.
#[tokio::test]
async fn rotation_pending_defers_a_host_blob_changeset_until_adoption() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    // The live cipher is generation 1; the cloud has committed generation 2.
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([8u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");
    let write_id = db
        .pending_writes()
        .await
        .expect("read rotation-paused Store write")
        .into_iter()
        .next()
        .expect("host write is queued")
        .write_id;

    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2).unwrap();
    storage.open_into(&db).await.expect("open exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");

    run_single_sync_cycle(
        &storage.storage,
        "M",
        hlc.as_ref(),
        &SystemClock,
        &db,
        enc.as_ref(),
        &pending_rotation,
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert!(
        pending_write_count(&db).await > 0,
        "the host-blob changeset stays queued while sealing is paused",
    );
    let activated_bindings: i64 = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM row_blob_locators
                 WHERE table_name = 'note_photos' AND row_id = 'hponly'
                   AND column_name = 'id' AND row_stamp = '0000000001000-0000-M'",
                [],
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("count exact host-blob bindings");
    assert_eq!(
        activated_bindings, 0,
        "rotation pause installs no activated host-blob binding",
    );
    let exact_outbox_rows: i64 = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM cloud_outbox
                 WHERE operation = 'upload' AND table_name = 'note_photos'
                   AND row_id = 'hponly' AND column_name = 'id'
                   AND row_stamp = '0000000001000-0000-M'",
                [],
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("count exact host-blob upload handoffs");
    assert_eq!(
        exact_outbox_rows, 0,
        "rotation pause creates neither a cloud upload nor a Created handoff",
    );
    assert_eq!(
        crate::blob::local_files::read(&ld, "photos", "hponly", 5)
            .await
            .expect("read rotation-paused local blob"),
        Some(b"cover".to_vec()),
        "the pending Store write retains its exact local blob source",
    );

    // Adoption clears the pause (a fresh, unmarked rotation gate); the first cycle
    // after publishes the queued changeset and uploads its blob.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        pending_write_count(&db).await,
        0,
        "the queued changeset publishes on the first cycle after adoption",
    );
    let published = match db
        .write_status(&write_id)
        .await
        .expect("read adopted Store write status")
    {
        crate::WriteStatus::Published(position) => match *position {
            crate::PublishedPosition::MergeConcurrent { commit, .. } => commit,
            position => panic!("adopted Store write has wrong policy: {position:?}"),
        },
        status => panic!("adopted Store write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(
        db.exact_materialized_ref(&stream_id, published.coord.sequence())
            .await
            .expect("read adopted exact Store position")
            .is_some(),
        "the published Store write is materialized",
    );
    let activated = stored_blob_for_row(&db, "note_photos", "hponly")
        .await
        .expect("adoption activates the exact host-blob binding");
    storage
        .storage
        .verify_blob_object(&activated)
        .await
        .expect("the activated host blob reads back exactly");
    assert_eq!(
        crate::local_blob::read(
            &ld.cache_blob_path("photos", activated.locator().locator_hash())
                .expect("host-blob cache path"),
        )
        .await
        .expect("read adopted host-blob cache"),
        b"cover",
        "CacheEager policy retains the published blob in the evictable cache",
    );
    assert!(
        crate::blob::local_files::read(&ld, "photos", "hponly", 5)
            .await
            .expect("read adopted local source")
            .is_none(),
        "publication removes the superseded local source",
    );
}

/// The sibling of the host-blob-changeset case for the other newly-gated seal
/// path: a ready host-provided make_remote intent. With a rotation pending,
/// `complete_host_provided_make_remotes` is skipped — the root's gate does not
/// flip, its blob is not sealed, and the intent stays queued — yet the cycle
/// completes. The first cycle after adoption flips the gate, uploads the blob,
/// and consumes the intent. Without the gate this cycle would abort at
/// `cipher_for_seal` before the pull.
#[tokio::test]
async fn rotation_pending_defers_a_ready_make_remote_intent_until_adoption() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([9u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Release', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");
    crate::blob::transition::make_remote(&db, &ld, hlc.as_ref(), "notes", "n1", false)
        .await
        .expect("queue the host-provided make_remote intent");

    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2).unwrap();
    storage.open_into(&db).await.expect("open exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");

    run_single_sync_cycle(
        &storage.storage,
        "M",
        hlc.as_ref(),
        &SystemClock,
        &db,
        enc.as_ref(),
        &pending_rotation,
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert_eq!(
        query_text(
            &db,
            "SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'"
        )
        .await,
        "0",
        "the make_remote gate does not flip while sealing is paused",
    );
    assert!(
        make_remote_intent_present(&db, "notes", "n1").await,
        "the make_remote intent stays queued while sealing is paused",
    );
    assert!(
        !stored_blob_exists(&db, &storage, "note_photos", "hponly").await,
        "no host-provided blob is sealed to the cloud while sealing is paused",
    );

    // Adoption clears the pause; the first cycle after completes the intent.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        query_text(
            &db,
            "SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'"
        )
        .await,
        "1",
        "the make_remote gate flips on the first cycle after adoption",
    );
    assert!(
        !make_remote_intent_present(&db, "notes", "n1").await,
        "completing the make_remote consumes its intent",
    );
    assert!(
        stored_blob_exists(&db, &storage, "note_photos", "hponly").await,
        "the host-provided blob uploads on the first cycle after adoption",
    );
}

#[tokio::test]
async fn ready_make_remote_provider_transport_is_offline() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [23u8; 32],
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = cycle_test_store(&db, &keypair).await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('transport-root', 'Root', NULL, 0, \
                 '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('transport-blob', 'transport-root', 'cover', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "transport-blob", b"cover")
        .await
        .expect("store host-provided blob");
    crate::blob::transition::make_remote(&db, &ld, &hlc, "notes", "transport-root", false)
        .await
        .expect("queue make_remote intent");
    fail_exact_create_on(&storage, 1);
    storage.open_into(&db).await.expect("open exact test Store");

    let failed = run_single_sync_cycle(
        &storage.storage,
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect_err("provider transport prevents make_remote completion");

    assert!(
        failed.contains("forced failure before exact create call 1"),
        "unexpected ready make_remote failure: {failed}"
    );
    assert!(
        failed.is_offline(),
        "make_remote transport is offline: {failed}"
    );
}

#[tokio::test]
async fn captured_changeset_retry_recognizes_first_blob_uploaded_before_second_failed() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([18u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    retain_store_packages_for_assertion(&db, &storage, b"captured-changeset-blob-retry").await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('firstblob', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('secondblob', 'n1', 'cover', 6, '{}', '0000000001001-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"first"),
            crate::blob::content_hash(b"second"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "firstblob", b"first")
        .await
        .expect("store first host-provided blob");
    crate::blob::local_files::store(&ld, "photos", "secondblob", b"second")
        .await
        .expect("store second host-provided blob");

    storage.open_into(&db).await.expect("open exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let reject_second_blob = Arc::new(CycleStorageInterceptor::reject_blob_create_on(
        Arc::clone(&storage),
        2,
    ));
    let failed = match run_cycle_in_task(
        Arc::clone(&reject_second_blob),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    {
        Ok(_) => panic!("second blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.to_string().contains("unexpected blob create call 2"),
        "cycle surfaces the second blob upload failure: {failed}"
    );
    let attempted_blobs = reject_second_blob.rejected_blobs();
    assert_eq!(attempted_blobs.len(), 2);
    storage
        .storage
        .verify_blob_object(&attempted_blobs[0])
        .await
        .expect("the first exact blob reached cloud before the second failed");
    assert!(storage
        .storage
        .verify_blob_object(&attempted_blobs[1])
        .await
        .is_err());
    assert!(
        crate::blob::local_files::read(&ld, "photos", "firstblob", 5)
            .await
            .expect("read first local")
            .is_some(),
        "the first local copy remains because the changeset was not published"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("two-blob retry cycle succeeds");
    let stream_id = local_store_stream_id(&db).await;
    assert!(db
        .exact_materialized_ref(&stream_id, 2)
        .await
        .expect("read retried exact materialized Store commit")
        .is_some());
    let activated_first = stored_blob_for_row(&db, "note_photos", "firstblob")
        .await
        .expect("retry activates the first exact blob binding");
    let activated_second = stored_blob_for_row(&db, "note_photos", "secondblob")
        .await
        .expect("retry activates the second exact blob binding");
    let attempted_first = attempted_blobs
        .iter()
        .find(|blob| blob.locator().blob_id() == "firstblob")
        .expect("first blob was attempted");
    let attempted_second = attempted_blobs
        .iter()
        .find(|blob| blob.locator().blob_id() == "secondblob")
        .expect("second blob was attempted");
    assert_eq!(&activated_first, attempted_first);
    assert_eq!(&activated_second, attempted_second);
    storage
        .storage
        .verify_blob_object(&activated_first)
        .await
        .expect("the first exact blob remains readable after retry");
    storage
        .storage
        .verify_blob_object(&activated_second)
        .await
        .expect("the second exact blob is readable after retry");
}

#[tokio::test]
async fn already_uploaded_host_blob_publishes_without_local_copy_or_reupload() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([19u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    retain_store_packages_for_assertion(&db, &storage, b"already-uploaded-host-blob").await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let pass_through = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('remoteonly', 'n1', 'cover', 15, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"already durable"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "remoteonly", b"already durable")
        .await
        .expect("store the first publication's host-provided blob");

    run_cycle_in_task(
        Arc::clone(&pass_through),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("first host blob cycle succeeds");
    let stream_id = local_store_stream_id(&db).await;
    assert!(db
        .exact_materialized_ref(&stream_id, 1)
        .await
        .expect("read first exact materialized Store commit")
        .is_some());
    let published_blob = db
        .row_blob_ref("note_photos", "remoteonly")
        .await
        .expect("read first exact remote blob binding")
        .stored()
        .cloned()
        .expect("first publication installs an exact remote blob binding");
    storage
        .storage
        .verify_blob_object(&published_blob)
        .await
        .expect("read back the first exact remote blob object");
    assert!(
        crate::blob::local_files::read(&ld, "photos", "remoteonly", 15)
            .await
            .expect("read cache-lazy host blob after publication")
            .is_none(),
        "the first publication removes the cache-lazy local copy",
    );
    host_exec(
        &db,
        "UPDATE note_photos \
         SET _updated_at = '0000000002000-0000-M' \
         WHERE id = 'remoteonly'",
    )
    .await;

    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(
        Arc::clone(&reject_blob_create),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("already-uploaded host blob cycle succeeds");
    assert!(db
        .exact_materialized_ref(&stream_id, 2)
        .await
        .expect("read re-emitted exact materialized Store commit")
        .is_some());
    assert!(reject_blob_create.rejected_blobs().is_empty());
    let republished_blob = db
        .row_blob_ref("note_photos", "remoteonly")
        .await
        .expect("read re-emitted exact remote blob binding")
        .stored()
        .cloned()
        .expect("re-emission retains an exact remote blob binding");
    assert_eq!(republished_blob, published_blob);
    storage
        .storage
        .verify_blob_object(&republished_blob)
        .await
        .expect("read back the re-emitted exact remote blob object");
}

#[tokio::test]
async fn fresh_push_failure_keeps_cache_lazy_local_copy_until_retry_publishes() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([20u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    retain_store_packages_for_assertion(&db, &storage, b"fresh-push-retry").await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('lazyblob', 'n1', 'cover', 4, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"lazy"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "lazyblob", b"lazy")
        .await
        .expect("store cache-lazy host-provided blob");
    let pending = db.pending_writes().await.expect("read pending Store write");
    let write_id = pending
        .iter()
        .find(|write| {
            write
                .affected_rows
                .iter()
                .any(|row| row.table == "note_photos" && row.primary_key == "lazyblob")
        })
        .expect("the blob host transaction has a durable Store write")
        .write_id
        .clone();

    fail_exact_create_on(&storage, 1);
    let error = run_cycle_in_task(
        Arc::clone(&cycle_storage),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect_err("the first Store package append fails");
    assert_eq!(
        error.to_string(),
        "publish Store write: storage operation failed: InMemoryCloudHome: forced failure before exact create call 1",
        "cycle surfaces the exact Store package append failure",
    );
    let prepared = db
        .oldest_prepared_store_write()
        .await
        .expect("read outbound Store queue")
        .expect("the exact prepared Store write remains durable");
    assert_ne!(
        prepared.commit.value.write_id, write_id,
        "the failed predecessor remains prepared ahead of the blob write",
    );
    assert!(
        crate::blob::local_files::read(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local")
            .is_some(),
        "the local copy remains until the changeset is published"
    );

    run_cycle_in_task(
        cycle_storage,
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect("prepared Store write retry succeeds");
    let status = db
        .write_status(&write_id)
        .await
        .expect("read retried Store write status");
    let commit = match status {
        crate::WriteStatus::Published(position) => match *position {
            crate::PublishedPosition::MergeConcurrent {
                device_id: published_device,
                commit,
            } if published_device == device_id => commit,
            position => panic!("retried Store write has wrong position: {position:?}"),
        },
        status => panic!("retried Store write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    let published = db
        .exact_materialized_ref(&stream_id, commit.coord.sequence())
        .await
        .expect("read retried exact Store position")
        .expect("retried exact Store position is materialized");
    assert_eq!(published, commit);
    let (published_ref, published_commit) =
        load_exact_materialized_commit(&db, &storage.storage, &stream_id, commit.coord.sequence())
            .await
            .expect("load retried exact Store commit")
            .expect("retried exact Store commit exists");
    assert_eq!(published_ref, published);
    assert_eq!(published_commit.value.write_id, write_id);
    assert!(
        published_commit.value.store_package().is_some(),
        "the blob Store write carries an exact Store package reference",
    );
    let activated_blob = stored_blob_for_row(&db, "note_photos", "lazyblob")
        .await
        .expect("retry activates the exact cache-lazy blob binding");
    storage
        .storage
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry leaves the exact cache-lazy blob readable");
    assert!(
        crate::blob::local_files::read(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local after publish")
            .is_none(),
        "the local copy drops after the prepared write retry commits"
    );
}

/// The acknowledgement the cycle writes records its completion time as an RFC
/// 3339 wall-clock string, never the HLC string used to order row writes.
async fn assert_latest_ack_timestamp_is_rfc3339(db: &Database, storage: &TestStore) {
    let published = db
        .latest_local_store_ack()
        .await
        .expect("read latest exact Store acknowledgement")
        .expect("the cycle published an acknowledgement");
    let root = db
        .local_store_root_ref()
        .await
        .expect("read exact Store root")
        .expect("exact Store root exists");
    let local_device = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registration = db
        .activated_store_device_registration_records()
        .await
        .expect("read activated Store registrations")
        .into_iter()
        .find(|(_, registration)| registration.device_id.to_string() == local_device)
        .expect("local Store registration is active")
        .1;
    let acknowledgement = crate::sync::store_objects::load_store_ack_ref(
        &storage.storage,
        &root,
        &published.reference,
        &registration,
    )
    .await
    .expect("load exact Store acknowledgement")
    .value;
    assert!(
        chrono::DateTime::parse_from_rfc3339(&acknowledgement.last_sync).is_ok(),
        "acknowledgement completion time must be RFC 3339, got {:?}",
        acknowledgement.last_sync,
    );
}

/// The main-push and post-pull paths stamp the acknowledgement with an RFC 3339
/// `last_sync`.
#[tokio::test]
async fn push_cycle_writes_rfc3339_ack_timestamp() {
    let db = open_test_db();
    let keypair = UserKeypair::generate();
    let storage = cycle_test_store(&db, &keypair).await;
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [21u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());
    retain_store_packages_for_assertion(&db, &storage, b"push-cycle-head-timestamp").await;

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = db
        .pending_writes()
        .await
        .expect("read push-timestamp write")
        .into_iter()
        .next()
        .expect("push-timestamp write is pending")
        .write_id;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    let published = match db
        .write_status(&write_id)
        .await
        .expect("read push-timestamp write status")
    {
        crate::WriteStatus::Published(position) => match *position {
            crate::PublishedPosition::MergeConcurrent { commit, .. } => commit,
            position => panic!("push-timestamp write has wrong policy: {position:?}"),
        },
        status => panic!("push-timestamp write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(db
        .exact_materialized_ref(&stream_id, published.coord.sequence())
        .await
        .expect("read push-timestamp materialization")
        .is_some());
    assert_latest_ack_timestamp_is_rfc3339(&db, &storage).await;
}

/// Snapshot metadata records its creation time as RFC 3339.
#[tokio::test]
async fn snapshot_cycle_writes_rfc3339_metadata_timestamp() {
    let keypair = UserKeypair::generate();
    let db = open_test_db();
    let storage = cycle_test_store(&db, &keypair).await;
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [22u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    let snapshot = latest_store_snapshot_meta(&db)
        .await
        .expect("the cycle published one snapshot");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&snapshot.created_at).is_ok(),
        "snapshot creation time must be RFC 3339, got {:?}",
        snapshot.created_at,
    );
}

#[tokio::test]
async fn merge_snapshot_count_cadence_uses_the_local_stream_coverage() {
    tokio::spawn(async {
        let owner = UserKeypair::generate();
        let db = open_test_db();
        let storage = cycle_test_store(&db, &owner).await;
        let source = open_test_db();
        let changeset = capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('cadence', 'Cadence', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
            ],
        )
        .await;

        storage
            .publish_changeset("local", 1, &changeset, SCHEMA_VERSION)
            .await
            .expect("publish local Store commit before peer setup");
        let mut peer_at_snapshot = None;
        for sequence in 1..=6 {
            peer_at_snapshot = Some(
                storage
                    .publish_changeset("peer", sequence, &changeset, SCHEMA_VERSION)
                    .await
                    .expect("publish peer Store commit before snapshot"),
            );
        }
        let peer_at_snapshot = peer_at_snapshot.expect("peer Store stream reaches sequence 6");
        let (_peer_temp, peer_store_dir) = temp_store_dir();
        pull_into(&db, &storage, &peer_store_dir).await;
        let local_at_snapshot = db
            .latest_local_store_position()
            .await
            .expect("read local Store position after peer setup")
            .expect("local Store stream has an exact snapshot position");
        let local_snapshot_sequence = local_at_snapshot.coord.sequence();
        let local_stream = match &local_at_snapshot.coord {
            crate::sync::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
                *stream_id
            }
            crate::sync::store_commit::StoreCommitCoord::Serial { .. } => {
                panic!("MergeConcurrent local fixture published a Serial commit")
            }
        };
        let peer_stream = match &peer_at_snapshot.coord {
            crate::sync::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
                *stream_id
            }
            crate::sync::store_commit::StoreCommitCoord::Serial { .. } => {
                panic!("MergeConcurrent peer fixture published a Serial commit")
            }
        };
        let membership = storage
            .open_into(&db)
            .await
            .expect("open Store before publishing cadence snapshot");
        crate::sync::store_snapshot::push_store_snapshot(
            &storage.storage,
            storage.store_root_hash(),
            crate::sync::snapshot::CreatedSnapshot {
                db_image: b"cadence-snapshot".to_vec(),
                blobs: Vec::new(),
            },
            crate::sync::store_commit::CommitFrontier::MergeConcurrent(BTreeMap::from([
                (local_stream, local_at_snapshot),
                (peer_stream, peer_at_snapshot),
            ])),
            db.schema_version(),
            &owner,
            T0.to_string(),
            Some(&membership),
            &db,
        )
        .await
        .expect("publish cadence snapshot");

        let local_after_snapshot = local_snapshot_sequence
            .checked_add(100)
            .expect("local snapshot cadence sequence does not overflow");
        for sequence in local_snapshot_sequence + 1..=local_after_snapshot {
            storage
                .publish_changeset("local", sequence, &changeset, SCHEMA_VERSION)
                .await
                .expect("publish local Store commit after snapshot");
        }
        assert_eq!(
            db.latest_local_store_position()
                .await
                .expect("read latest local Store commit")
                .expect("local Store stream has commits")
                .coord
                .sequence(),
            local_after_snapshot,
        );

        let unregistered_member = UserKeypair::generate();
        crate::sync::membership_ops::invite_member(
            &storage.storage,
            storage.home.as_ref(),
            &owner,
            &Hlc::new("local".to_string()),
            &pubkey_hex(&unregistered_member),
            None,
            crate::sync::membership::MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "test-lib",
            "Test Store",
            &db,
        )
        .await
        .expect("invite unregistered member to hold back package reclamation");

        let (_temp, store_dir) = temp_store_dir();
        let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
            [24u8; 32],
        )));
        run_cycle_m(
            &storage,
            &db,
            &cipher,
            &owner,
            &Hlc::new("local".to_string()),
            &store_dir,
        )
        .await;

        assert_eq!(
            db.latest_local_store_snapshot()
                .await
                .expect("read latest Store snapshot")
                .expect("count cadence publishes a Store snapshot")
                .reference
                .generation,
            1,
        );
    })
    .await
    .expect("snapshot cadence orchestration completes");
}

/// The prepared-write retry stamps the acknowledgement with an RFC 3339
/// `last_sync`.
#[tokio::test]
async fn prepared_write_retry_writes_rfc3339_ack_timestamp() {
    let db = open_test_db();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([23u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));
    retain_store_packages_for_assertion(&db, &storage, b"prepared-retry-head-timestamp").await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read prepared-retry device")
        .expect("prepared-retry device exists");

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // The first push fails at the package append, so the prepared write remains
    // owned by its durable record and no head is written for it yet.
    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect_err("the first Store package append fails");
    assert!(
        db.oldest_prepared_store_write()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact Store batch remains durable after append failure",
    );

    // The next cycle retries the prepared write.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        enc,
        keypair,
        hlc,
        ld,
        device_id,
    )
    .await
    .expect("retry prepared Store write");
    assert!(db
        .oldest_prepared_store_write()
        .await
        .expect("read retried outbound Store queue")
        .is_none());
    assert_latest_ack_timestamp_is_rfc3339(&db, &storage).await;
}

#[tokio::test]
async fn missing_user_blob_blocks_prepared_write_before_publish() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([10u8; 32]),
    )));
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    storage.open_into(&db).await.expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.storage,
        &db,
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize MergeConcurrent test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let planted = create_exact_blob(&storage, "audio", "audio1", b"AUDIO").await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('audio1', 'n1', 'audio', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"AUDIO"),
        ),
    )
    .await;

    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(
        Arc::clone(&cycle_storage),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect_err("the first Store package append fails");
    let first_write_id = db
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write")
        .expect("the exact Store write remains after append failure")
        .commit
        .value
        .write_id;
    assert!(!local_store_package_exists(&db, &storage, 2).await);

    storage
        .storage
        .delete_blob_object(&planted)
        .await
        .expect("delete exact user-provided blob");
    let retry = run_cycle_in_task(
        Arc::clone(&cycle_storage),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await;
    let err = match retry {
        Err(err) => err,
        Ok(_) => panic!("prepared write must recheck the remote user-provided blob"),
    };

    assert!(
        err.to_string()
            .contains("prepare Store write: outbound blob audio/audio1 is absent from storage"),
        "prepared write surfaces the missing blob: {err}",
    );
    let first_write_status = db
        .write_status(&first_write_id)
        .await
        .expect("read first write status");
    assert!(
        matches!(
            &first_write_status,
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::MergeConcurrent { commit, .. }
                        if commit.coord.sequence() == 1
                )
        ),
        "first write status after blocking its successor: {first_write_status:?}",
    );
    let pending = db.pending_writes().await.expect("read pending writes");
    assert_eq!(pending.len(), 1);
    let blocked_write_id = pending[0].write_id.clone();
    let blocked = crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
        namespace: "audio".to_string(),
        id: "audio1".to_string(),
    });
    assert_eq!(pending[0].status, blocked);
    assert!(
        !local_store_package_exists(&db, &storage, 2).await,
        "the blocked write has no package or head",
    );

    let _restored = create_exact_blob(&storage, "audio", "audio1", b"AUDIO").await;
    run_cycle_in_task(
        cycle_storage,
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("restored missing user blob cycle succeeds");
    assert_eq!(
        db.write_status(&blocked_write_id)
            .await
            .expect("read blocked write status"),
        blocked,
        "a semantic block is not retried by reconnect",
    );
    assert!(!local_store_package_exists(&db, &storage, 2).await);
}

#[tokio::test]
async fn outgoing_preparation_failure_keeps_pending_write_for_retry() {
    let keypair = UserKeypair::generate();
    let hlc = Arc::new(Hlc::new("M".to_string()));
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([9u8; 32]),
    )));
    let db = open_test_db();
    let storage = Arc::new(cycle_test_store(&db, &keypair).await);
    retain_store_packages_for_assertion(&db, &storage, b"outgoing-preparation-retry").await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('prepare-fail', 'Prepare Fail', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = db
        .pending_writes()
        .await
        .expect("read preparation-failure write")
        .into_iter()
        .next()
        .expect("preparation-failure write is pending")
        .write_id;

    db.call(|conn| {
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_outbound_preparation \
             BEFORE UPDATE OF prepared ON store_writes \
             WHEN OLD.prepared IS NULL AND NEW.prepared IS NOT NULL \
             BEGIN SELECT RAISE(ABORT, 'injected Store preparation failure'); END;",
        )
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("install Store preparation fault");
    storage.open_into(&db).await.expect("open exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact preparation-failure device")
        .expect("exact preparation-failure device exists");
    let failed = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id.clone(),
    )
    .await
    .expect_err("outgoing preparation should fail");
    assert!(
        failed.contains("injected Store preparation failure"),
        "cycle surfaces the outgoing preparation failure: {failed}"
    );
    assert_eq!(
        pending_write_count(&db).await,
        1,
        "the pending write remains queued when outgoing preparation fails"
    );
    assert_eq!(
        db.write_status(&write_id)
            .await
            .expect("read failed-preparation write status"),
        crate::WriteStatus::Pending,
    );

    db.call(|conn| {
        conn.execute_batch("DROP TRIGGER fail_outbound_preparation")
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("remove Store preparation fault");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("retry outgoing preparation");
    let published = match db
        .write_status(&write_id)
        .await
        .expect("read retried preparation write status")
    {
        crate::WriteStatus::Published(position) => match *position {
            crate::PublishedPosition::MergeConcurrent { commit, .. } => commit,
            position => panic!("retried preparation has wrong policy: {position:?}"),
        },
        status => panic!("retried preparation write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(db
        .exact_materialized_ref(&stream_id, published.coord.sequence())
        .await
        .expect("read retried preparation materialization")
        .is_some());
    assert_eq!(
        pending_write_count(&db).await,
        0,
        "the pending write leaves the pending set after publication"
    );
}

/// Like [`run_cycle_m`] but over an arbitrary `&dyn SyncStorage` (e.g. the
/// host-write injector), still with no cloud home (no outbox drain, no auth
/// refresh).
async fn run_cycle_m_storage(
    storage: &CycleStorageInterceptor,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    storage
        .inner
        .open_into(db)
        .await
        .expect("open exact test Store");
    cycle::ensure_owner_anchored_chain(
        &storage.inner.storage,
        db,
        &storage.inner.root,
        storage.inner.protocol_root(),
        &storage.inner.protocol_founder_keypair(),
    )
    .await
    .expect("initialize MergeConcurrent test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact test device id")
        .expect("exact test device id exists");
    run_single_sync_cycle(
        storage,
        &device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        &PendingRotation::none(),
        keypair,
        None,
        ld,
        None,
        None,
    )
    .await
    .expect("cycle");
}

// ---- changeset reclamation through a real cycle ----

/// A package remains available after it becomes snapshot-covered and acknowledged
/// while an accepted Merge materialization still needs it for replay. Peer A has
/// pushed A/1; M pulls it, acknowledges it, snapshots it, and retains its package.
///
/// The mock is built with M's keypair so the head it signs for M and the ack M
/// publishes share an author, the same identity a real device's storage and ack
/// share — which is what lets reclamation honor M's ack against M's head.
#[tokio::test]
async fn cycle_preserves_a_fully_acked_changeset_retained_for_replay() {
    let keypair = UserKeypair::generate();
    let db_m = open_test_db();
    let storage = Arc::new(cycle_test_store(&db_m, &keypair).await);
    let (_tmp, ld) = temp_store_dir();
    let enc = Arc::new(RwLock::new(CloudCipher::Encrypted(
        EncryptionService::from_key([11u8; 32]),
    )));
    let hlc = Arc::new(Hlc::new("M".to_string()));

    // Peer A's changeset 1 (a shareable note).
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    let published = storage
        .publish_changeset("A", 1, &a_cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let published_stream = match &published.coord {
        crate::sync::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
            *stream_id
        }
        crate::sync::store_commit::StoreCommitCoord::Serial { .. } => {
            panic!("MergeConcurrent fixture published a Serial commit")
        }
    };
    let stream_id = published_stream.to_string();

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    let device_id = db_m
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        db_m.clone(),
        Arc::clone(&enc),
        keypair.clone(),
        Arc::clone(&hlc),
        ld.clone(),
        device_id,
    )
    .await
    .expect("retained-replay cycle succeeds");

    let snapshot = latest_store_snapshot_meta(&db_m)
        .await
        .expect("the reclamation cycle publishes a covering snapshot");
    assert!(matches!(
        &snapshot.coverage,
        crate::sync::store_commit::CommitFrontier::MergeConcurrent(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    let ack_ref = db_m
        .latest_local_store_ack()
        .await
        .expect("read reclamation acknowledgement")
        .expect("the reclamation cycle publishes an acknowledgement")
        .reference;
    let root = db_m
        .local_store_root_ref()
        .await
        .expect("read reclamation Store root")
        .expect("reclamation Store root exists");
    let local_device = db_m
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registrations = db_m
        .activated_store_device_registration_records()
        .await
        .expect("read reclamation Store registrations");
    assert_eq!(registrations.len(), 1, "{registrations:?}");
    let registration = registrations
        .into_iter()
        .find(|(_, registration)| registration.device_id.to_string() == local_device)
        .expect("local reclamation Store registration is active")
        .1;
    let acknowledgement = crate::sync::store_objects::load_store_ack_ref(
        &storage.storage,
        &root,
        &ack_ref,
        &registration,
    )
    .await
    .expect("load exact reclamation acknowledgement")
    .value;
    assert!(matches!(
        &acknowledgement.store_cut,
        crate::sync::store_commit::StoreHistoryCut::MergeConcurrent(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    assert!(
        store_package_exists(&db_m, &storage, &stream_id, 1).await,
        "the accepted Merge materialization retains its Store package for replay",
    );
}

/// Reclamation refuses the snapshot proof while one exact active device is
/// behind it. The behind device acknowledges the first data commit, the owner
/// publishes another, and both packages remain available for its later pull.
#[tokio::test]
async fn cycle_preserves_packages_until_every_device_covers_the_snapshot() {
    Box::pin(run_cycle_preserves_packages_until_every_device_covers_the_snapshot()).await;
}

async fn run_cycle_preserves_packages_until_every_device_covers_the_snapshot() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [12u8; 32],
    )));
    let hlc = Hlc::new("owner".to_string());

    let source = open_test_db();
    let first_changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a1', 'Title Alpha', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage
        .publish_changeset("owner", 1, &first_changeset, SCHEMA_VERSION)
        .await
        .expect("publish first exact Store changeset");

    let behind = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&behind),
        None,
        crate::sync::membership::MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact behind Member identity");
    let behind_db = open_test_db();
    activate_joined_test_device(&storage, &owner_db, &behind_db, &behind).await;
    pull_into(&behind_db, &storage, &ld).await;

    let behind_frontier = crate::sync::store_commit::CommitFrontier::from_refs(
        crate::WritePolicy::MergeConcurrent,
        behind_db
            .materialized_frontier()
            .await
            .expect("read behind device frontier"),
    )
    .expect("validate behind device frontier");
    crate::sync::store_engine::stage_merge_acknowledgement_for_test(
        &behind_db,
        &storage.storage,
        behind_frontier,
        T0.to_string(),
        &behind,
    )
    .await
    .expect("stage behind device acknowledgement");
    crate::sync::store_engine::drain_merge_acknowledgements_for_test(
        &behind_db,
        &storage.storage,
        &behind,
    )
    .await
    .expect("publish behind device acknowledgement");

    let second_changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a2', 'Title Beta', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    let second_sequence = owner_db
        .latest_local_store_position()
        .await
        .expect("read owner Store position after registration activation")
        .expect("registration activation advances the owner Store stream")
        .coord
        .sequence()
        .checked_add(1)
        .expect("owner Store sequence remains representable");
    storage
        .publish_changeset("owner", second_sequence, &second_changeset, SCHEMA_VERSION)
        .await
        .expect("publish second exact Store changeset after registration activation");

    drop(source);
    tokio::spawn(async move {
        let (owner_registration, registration) = owner_db
            .local_blob_write_authority()
            .await
            .expect("read owner announcement authority");
        let owner_stream = registration
            .store_announcement_activation(&owner_registration)
            .expect("derive owner Store announcement activation")
            .author_stream_id()
            .to_string();
        let second_sequence = owner_db
            .latest_local_store_position()
            .await
            .expect("read published owner Store position")
            .expect("second owner Store commit is materialized")
            .coord
            .sequence();
        run_cycle_m(&storage, &owner_db, &enc, &owner, &hlc, &ld).await;

        assert!(
            store_package_exists(&owner_db, &storage, &owner_stream, 1).await,
            "reclamation keeps the earlier package while an active device is behind",
        );
        assert!(
            store_package_exists(&owner_db, &storage, &owner_stream, second_sequence).await,
            "reclamation keeps the package the behind device still needs",
        );

        pull_into(&behind_db, &storage, &ld).await;
        assert!(
            row_exists(&behind_db, "SELECT 1 FROM notes WHERE id = 'a2'").await,
            "the behind device pulls the retained changeset",
        );
    })
    .await
    .expect("snapshot coverage reclamation cycle task");
}

/// A registered Member publishes rows but cannot author a catalog snapshot.
#[tokio::test]
async fn member_device_does_not_create_a_snapshot() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = Arc::new(cycle_test_store(&owner_db, &owner).await);
    let (_tmp, ld) = temp_store_dir();
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");

    let member_db = open_test_db();
    activate_joined_test_device(&storage, &owner_db, &member_db, &member).await;
    let encryption = Arc::new(RwLock::new(CloudCipher::Encrypted(encryption)));
    host_exec(
        &member_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-member', '2026-01-01')",
    )
    .await;

    let member_device_id = member_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Member Store device")
        .expect("Member Store device exists");
    let member_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    run_cycle_in_task(
        member_storage,
        member_db.clone(),
        Arc::clone(&encryption),
        member.clone(),
        Arc::new(Hlc::new("member".to_string())),
        ld.clone(),
        member_device_id,
    )
    .await
    .expect("Member Store cycle succeeds");

    assert!(
        local_store_package_exists(&member_db, &storage, 1).await,
        "the Member's row publishes through its exact Store stream",
    );
    assert!(
        latest_store_snapshot_meta(&member_db).await.is_none(),
        "a Member device cannot author catalog snapshot metadata",
    );
}

#[tokio::test]
async fn same_principal_device_join_completes_on_the_runtime_stack() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([43; 32]);
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");

    let member_db = open_test_db();
    activate_joined_test_device(&storage, &owner_db, &member_db, &member).await;

    assert!(
        member_db
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the public join sequence activates the joining registration",
    );
}

struct SamePrincipalApprovalFixture {
    _pending_dir: tempfile::TempDir,
    pending: crate::sync::device_join::DeviceJoinJournalDatabase,
    authorization: crate::sync::device_join::DeviceJoinAuthorization,
    approval: crate::sync::device_join::DeviceProviderAdmissionApproval,
}

async fn prepare_same_principal_approval_fixture(
    owner_db: &Database,
    storage: &TestStore,
    owner: &UserKeypair,
    member: &UserKeypair,
    hlc_node: &str,
) -> SamePrincipalApprovalFixture {
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        owner,
        &Hlc::new(hlc_node.to_string()),
        &pubkey_hex(member),
        None,
        crate::sync::membership::MemberRole::Member,
        &EncryptionService::from_key([59; 32]),
        "test-lib",
        "Test Store",
        owner_db,
    )
    .await
    .expect("invite exact Member identity");
    let authorization = crate::sync::device_join::DeviceJoinAuthorization::MergeConcurrent(
        storage
            .open_into(owner_db)
            .await
            .expect("load exact Merge membership"),
    );
    let pending_dir = tempfile::tempdir().expect("create join directory");
    let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
        pending_dir.path().join("pending.sqlite"),
    )
    .expect("open join journal");
    let offer = crate::sync::device_join::begin_device_join(
        owner_db,
        &storage.storage,
        &authorization,
        owner,
        &pubkey_hex(member),
        storage
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone(),
    )
    .await
    .expect("begin exact device join");
    let access_request = crate::sync::device_join::prepare_device_provider_access_request(
        &pending,
        SyncStorage::provider_binding(&storage.storage)
            .await
            .expect("resolve provider binding"),
        member,
        offer,
    )
    .await
    .expect("prepare exact provider request");
    let approval = crate::sync::device_join::authorize_device_provider_access(
        owner_db,
        &storage.storage,
        None,
        None,
        None,
        &authorization,
        owner,
        access_request,
    )
    .await
    .expect("authorize exact provider access");
    SamePrincipalApprovalFixture {
        _pending_dir: pending_dir,
        pending,
        authorization,
        approval,
    }
}

#[tokio::test]
async fn provider_approval_rejects_access_activation_policy_that_differs_from_the_signed_store_root(
) {
    use crate::sync::device_join::{DeviceJoinRole, DeviceJoinStatus};

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let source = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "join-policy-member",
    )
    .await;
    let valid_approval = source.approval;

    let serial_db = open_serial_test_db();
    let serial_store = TestStore::create(&serial_db, "join-policy-source", owner.clone())
        .await
        .expect("create exact Serial Store");
    let mut provider_admin = valid_approval.request.offer.provider_admin.as_ref().clone();
    provider_admin.capability.serial_coordination = serial_store
        .protocol_root
        .descriptor
        .founder_provider_admin
        .capability
        .serial_coordination
        .clone();
    let (owner_registration_ref, owner_registration, owner_device_signer) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let valid_offer = valid_approval.request.offer.as_ref();
    let conflicting_offer = crate::sync::device_join::DeviceJoinOffer::signed(
        valid_offer.attempt_id,
        valid_offer.member_pubkey.clone(),
        valid_offer.store_root.clone(),
        valid_offer.provider.clone(),
        valid_offer.attempt_slot.clone(),
        valid_offer.outcome_slot.clone(),
        owner_registration_ref,
        valid_offer.owner_grant.clone(),
        provider_admin,
        &owner_registration,
        &owner_device_signer,
    )
    .expect("sign conflicting offer");
    let pending_dir = tempfile::tempdir().expect("create joining directory");
    let pending = crate::sync::device_join::DeviceJoinJournalDatabase::open(
        pending_dir.path().join("pending.sqlite"),
    )
    .expect("open joining journal");
    let conflicting_request = crate::sync::device_join::prepare_device_provider_access_request(
        &pending,
        SyncStorage::provider_binding(&storage.storage)
            .await
            .expect("resolve provider binding"),
        &member,
        conflicting_offer,
    )
    .await
    .expect("prepare conflicting provider request");
    let mut conflicting_grant = valid_approval.access_grant;
    conflicting_grant.activation.coord = crate::sync::store_commit::StoreCommitCoord::Serial {
        sequence: conflicting_grant.activation.coord.sequence(),
    };
    let verified_root =
        crate::sync::store_objects::load_store_protocol_root(&storage.storage, &storage.root)
            .await
            .expect("load exact signed Store root");
    let error = crate::sync::device_join::DeviceProviderAdmissionApproval::signed(
        conflicting_request.clone(),
        conflicting_grant.clone(),
        crate::sync::device_join::DeviceProviderAdmissionChallenge::SamePrincipal,
        &verified_root,
        &owner_registration,
        &owner_device_signer,
    )
    .expect_err("the pinned Merge root rejects a Serial access activation");
    assert!(matches!(
        error,
        crate::sync::device_join::DeviceJoinError::ApprovalMismatch
    ));
    let malformed_approval =
        crate::sync::device_join::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
            conflicting_request.clone(),
            conflicting_grant,
            crate::sync::device_join::DeviceProviderAdmissionChallenge::SamePrincipal,
            &owner_device_signer,
        );
    let consumer_error = crate::sync::device_join::prepare_device_registration_request(
        &pending,
        &storage.storage,
        None,
        None,
        &member,
        malformed_approval,
    )
    .await
    .expect_err("the production joiner rejects the signed malformed approval");
    assert!(matches!(
        consumer_error,
        crate::sync::device_join::DeviceJoinError::ApprovalMismatch
    ));
    assert!(matches!(
        crate::sync::device_join::load_pending_device_join_status(
            &pending,
            conflicting_request.offer.attempt_id,
        )
        .expect("load unchanged join status"),
        Some(DeviceJoinStatus::AwaitingProviderAdmission { request })
            if request == conflicting_request
    ));
    assert!(pending
        .load(conflicting_request.offer.attempt_id, DeviceJoinRole::Joiner,)
        .expect("load exact join journal")
        .is_some());
}

#[tokio::test]
async fn owner_accepts_access_activation_covered_by_a_later_predecessor_head() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let first = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "first-join-member",
    )
    .await;
    let first_registration_request = crate::sync::device_join::prepare_device_registration_request(
        &first.pending,
        &storage.storage,
        None,
        None,
        &member,
        first.approval,
    )
    .await
    .expect("prepare first registration request");

    let second_member = UserKeypair::generate();
    let second = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &second_member,
        "second-join-member",
    )
    .await;

    crate::sync::device_join::accept_device_registration_request(
        &owner_db,
        &storage.storage,
        None,
        &second.authorization,
        &owner,
        first_registration_request,
    )
    .await
    .expect("the later predecessor head covers the first access activation");
}

#[tokio::test]
async fn owner_signed_attempt_rejects_an_invalid_embedded_provider_approval() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let fixture = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "invalid-embedded-approval",
    )
    .await;
    let request = crate::sync::device_join::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
        None,
        None,
        &member,
        fixture.approval,
    )
    .await
    .expect("prepare exact registration request");
    let offer = request.approval.request.offer.as_ref();
    let local_device_id = owner_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load local device id")
        .expect("active founder device id");
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        &owner_db,
        &storage.storage,
        crate::sync::store_outbound::StoreOperationPreparation::MergeConcurrent {
            membership: fixture
                .authorization
                .merge_chain()
                .expect("Merge authorization"),
        },
        &local_device_id,
        &owner,
    )
    .await
    .expect("prepare exact Owner Store commit");
    let cut = plan.predecessor_cut().expect("load exact predecessor cut");
    let membership = plan.membership_state().clone();
    let (_, owner_registration, owner_device_signer) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let mut invalid_approval = request.approval.as_ref().clone();
    invalid_approval.signature.push('0');
    let attempt = crate::sync::store_commit::DeviceJoinAttempt::signed(
        offer.store_root.clone(),
        offer.attempt_id,
        offer.attempt_slot.clone(),
        request.expected_registration.clone(),
        request.registration_slot.clone(),
        offer.outcome_slot.clone(),
        cut,
        membership,
        offer.provider_admin.grant_id.clone(),
        invalid_approval,
        request.response.clone(),
        offer.owner_registration.clone(),
        offer.owner_grant.clone(),
        &owner_registration,
        &owner_device_signer,
    )
    .expect("Owner signs the attempt envelope");
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let prefix = crate::sync::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
    let prepared = storage
        .storage
        .prepare_protocol_object(
            &context,
            offer.attempt_slot.clone(),
            &prefix,
            attempt.to_bytes(),
        )
        .expect("prepare exact attempt object");
    storage
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish exact attempt object");
    let attempt_ref = crate::sync::store_commit::DeviceJoinAttemptRef {
        attempt_id: offer.attempt_id,
        attempt_hash: attempt.attempt_hash(),
        object: prepared.reference().clone(),
    };
    crate::sync::store_engine::load_verified_device_join_attempt_ref(
        &storage.storage,
        &offer.store_root,
        &attempt_ref,
        &owner_registration,
    )
    .await
    .expect_err("the complete attempt loader rejects the embedded approval signature");
}

#[tokio::test]
async fn owner_rejects_invalid_access_activation_without_consuming_the_join_journal() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let fixture = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "invalid-access-activation",
    )
    .await;
    let valid_request = crate::sync::device_join::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
        None,
        None,
        &member,
        fixture.approval,
    )
    .await
    .expect("prepare exact registration request");
    let mut invalid_access = valid_request.approval.access_grant.clone();
    invalid_access.activation.commit_hash =
        crate::sync::store_commit::ObjectHash::digest(b"absent provider-access activation");
    let malformed_approval =
        crate::sync::device_join::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
            valid_request.approval.request.as_ref().clone(),
            invalid_access,
            valid_request.approval.admission.clone(),
            &storage
                .founder_device_authority()
                .await
                .expect("load exact founder authority")
                .2,
        );
    let malformed_request = crate::sync::device_join::DeviceRegistrationRequest::signed(
        malformed_approval,
        valid_request.expected_registration.clone(),
        valid_request.registration_slot.clone(),
        valid_request.response.clone(),
        &member,
    )
    .expect("joiner signs malformed remote request fixture");
    crate::sync::device_join::accept_device_registration_request(
        &owner_db,
        &storage.storage,
        None,
        &fixture.authorization,
        &owner,
        malformed_request,
    )
    .await
    .expect_err("Owner rejects the absent exact provider-access activation");
    crate::sync::device_join::accept_device_registration_request(
        &owner_db,
        &storage.storage,
        None,
        &fixture.authorization,
        &owner,
        valid_request,
    )
    .await
    .expect("valid retry remains possible after rejected activation");
}

#[tokio::test]
async fn joiner_rejects_access_commit_beyond_another_streams_exclusion_cutoff() {
    Box::pin(async {
        let founder = UserKeypair::generate();
        let founder_db = open_test_db();
        let storage = cycle_test_store(&founder_db, &founder).await;
        let excluding_owner = UserKeypair::generate();
        let encryption = EncryptionService::from_key([62; 32]);
        crate::sync::membership_ops::invite_member(
            &storage.storage,
            storage.home.as_ref(),
            &founder,
            &Hlc::new("excluding-owner".to_string()),
            &pubkey_hex(&excluding_owner),
            None,
            crate::sync::membership::MemberRole::Member,
            &encryption,
            "test-lib",
            "Test Store",
            &founder_db,
        )
        .await
        .expect("invite second exact Owner identity");
        let excluding_db = open_test_db();
        crate::sync::test_helpers::install_active_device_fixture(
            &storage,
            &founder_db,
            &excluding_db,
            &excluding_owner,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate second Owner device");
        crate::sync::test_helpers::promote_active_member_fixture(
            &storage,
            &founder_db,
            &excluding_db,
            &founder,
            &excluding_owner,
            &encryption,
        )
        .await
        .expect("promote active second Owner");
        let founder_registration = storage
            .founder_device_authority()
            .await
            .expect("load exact founder authority")
            .0;
        let proposal = match crate::sync::store_device_exclusion::propose_device_exclusion(
        &excluding_db,
        &storage.storage,
        None,
        &excluding_owner,
        &founder_registration,
    )
    .await
    .expect("propose founder device exclusion")
    {
        crate::sync::store_device_exclusion::StoreDeviceExclusionResult::ProposalActivated {
            proposal,
            ..
        } => proposal,
        result => panic!("unexpected exclusion proposal result: {result:?}"),
    };

        let joining_member = UserKeypair::generate();
        let approval = prepare_same_principal_approval_fixture(
            &founder_db,
            &storage,
            &founder,
            &joining_member,
            "post-freeze-access",
        )
        .await;

        let frontier = crate::sync::store_commit::CommitFrontier::from_refs(
            excluding_db.write_policy(),
            excluding_db
                .materialized_frontier()
                .await
                .expect("load exclusion frontier"),
        )
        .expect("shape exclusion frontier");
        crate::sync::store_engine::stage_merge_acknowledgement_for_test(
            &excluding_db,
            &storage.storage,
            frontier,
            "2026-07-20T00:01:00Z".to_string(),
            &excluding_owner,
        )
        .await
        .expect("stage exclusion acknowledgement");
        crate::sync::store_engine::drain_merge_acknowledgements_for_test(
            &excluding_db,
            &storage.storage,
            &excluding_owner,
        )
        .await
        .expect("publish exclusion acknowledgement");
        match crate::sync::store_device_exclusion::finalize_device_exclusion(
            &excluding_db,
            &storage.storage,
            None,
            &excluding_owner,
            &proposal,
        )
        .await
        .expect("activate founder exclusion")
        {
            crate::sync::store_device_exclusion::StoreDeviceExclusionResult::OutcomeActivated {
                ..
            } => {}
            result => panic!("unexpected exclusion outcome result: {result:?}"),
        }

        crate::sync::device_join::prepare_device_registration_request(
            &approval.pending,
            &storage.storage,
            None,
            None,
            &joining_member,
            approval.approval,
        )
        .await
        .expect_err("the excluded founder suffix cannot authorize provider access");
    })
    .await;
}

#[tokio::test]
async fn authenticated_next_head_with_a_missing_commit_body_rejects_provider_access() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let fixture = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "missing-current-commit",
    )
    .await;
    let later_member = UserKeypair::generate();
    let later = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &later_member,
        "missing-current-commit-later-member",
    )
    .await;
    storage
        .storage
        .delete_protocol_object(&later.approval.access_grant.activation.object)
        .await
        .expect("remove the commit body behind its authenticated head");

    crate::sync::device_join::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
        None,
        None,
        &member,
        fixture.approval,
    )
    .await
    .expect_err("an authenticated head cannot hide its missing commit body");
}

#[tokio::test]
async fn unauthenticated_next_head_does_not_hide_the_prior_accepted_access_commit() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let fixture = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "garbage-next-head",
    )
    .await;
    let activation = fixture.approval.access_grant.activation.clone();
    let (owner_ref, owner_registration, _) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let (next_slot, _) = crate::sync::store_outbound::exact_next_announcement_slot(
        &storage.storage,
        &storage.root,
        &owner_ref,
        &owner_registration,
        Some(&activation),
    )
    .await
    .expect("load exact next announcement slot");
    let next_sequence = activation
        .coord
        .sequence()
        .checked_add(1)
        .expect("next sequence exists");
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::StoreHead,
    );
    let prefix = crate::sync::store_commit::head_slot_prefix(
        &owner_registration.device_id.to_string(),
        next_sequence,
    );
    let garbage = storage
        .storage
        .prepare_protocol_object(&context, next_slot, &prefix, b"not a signed head".to_vec())
        .expect("prepare unauthenticated next-head bytes");
    storage
        .storage
        .create_protocol_object(&garbage)
        .await
        .expect("publish unauthenticated next-head bytes");
    crate::sync::device_join::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
        None,
        None,
        &member,
        fixture.approval,
    )
    .await
    .expect("unauthenticated garbage leaves the prior accepted access commit current");
}

#[tokio::test]
async fn authenticated_malformed_next_head_rejects_prior_provider_access() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let fixture = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "signed-malformed-next-head",
    )
    .await;
    let activation = fixture.approval.access_grant.activation.clone();
    let (owner_ref, owner_registration, owner_device_signer) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let (next_slot, accepted_head_ref) = crate::sync::store_outbound::exact_next_announcement_slot(
        &storage.storage,
        &storage.root,
        &owner_ref,
        &owner_registration,
        Some(&activation),
    )
    .await
    .expect("load exact next announcement slot");
    let accepted_head_ref = accepted_head_ref.expect("activation has an accepted Store head");
    let accepted_head = crate::sync::store_objects::load_head_ref(
        &storage.storage,
        storage.root.store_root_hash,
        &accepted_head_ref,
        &owner_registration,
        &activation,
    )
    .await
    .expect("load accepted Store head");
    let next_sequence = activation
        .coord
        .sequence()
        .checked_add(1)
        .expect("next sequence exists");
    let crate::sync::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } =
        activation.coord
    else {
        panic!("Merge fixture produced a Serial activation");
    };
    let mut next_commit = activation;
    next_commit.coord = crate::sync::store_commit::StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence: next_sequence,
    };
    let stream_activation = owner_registration
        .store_announcement_activation(&owner_ref)
        .expect("derive founder announcement activation")
        .activation_id();
    let malformed = crate::sync::store_commit::StoreDeviceHead::signed(
        storage.root.store_root_hash,
        owner_ref,
        next_commit,
        accepted_head.value.history_summary,
        crate::sync::store_commit::SuccessorLink {
            activation: stream_activation,
            predecessor: None,
            next_slot: next_slot.clone(),
        },
        &owner_device_signer,
    )
    .expect("sign malformed successor chain");
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::StoreHead,
    );
    let prefix = crate::sync::store_commit::head_slot_prefix(
        &owner_registration.device_id.to_string(),
        next_sequence,
    );
    let prepared = storage
        .storage
        .prepare_protocol_object(&context, next_slot, &prefix, malformed.to_bytes())
        .expect("prepare authenticated malformed head");
    storage
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish authenticated malformed head");

    crate::sync::device_join::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
        None,
        None,
        &member,
        fixture.approval,
    )
    .await
    .expect_err("an authenticated malformed successor makes current history unverifiable");
}

#[tokio::test]
async fn pre_attempt_device_join_abandonment_is_observed_and_retry_safe() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([44; 32]);
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_pre_attempt_abandonment(&owner_db, &storage, &owner, &member).await;
}

#[tokio::test]
async fn post_attempt_device_join_cancellation_closes_and_cleans_up_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([45; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_post_attempt_cancellation(
        &owner_db,
        &storage,
        &owner,
        &member,
        JoinerCancellationDisposition::Closure,
    )
    .await;
}

#[tokio::test]
async fn pre_attempt_device_join_abandonment_is_retry_safe_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(&owner_db, "serial-device-join-abandonment", owner.clone())
        .await
        .expect("create exact Serial Store");

    exercise_pre_attempt_abandonment(&owner_db, &storage, &owner, &owner).await;
}

#[tokio::test]
async fn post_attempt_device_join_cancellation_closes_and_cleans_up_on_serial() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(
                run_post_attempt_device_join_cancellation_closes_and_cleans_up_on_serial(),
            )
            .await
            .expect("Serial post-attempt cancellation orchestration");
        })
        .await;
}

async fn run_post_attempt_device_join_cancellation_closes_and_cleans_up_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(&owner_db, "serial-device-join-cancellation", owner.clone())
        .await
        .expect("create exact Serial Store");

    exercise_post_attempt_cancellation(
        &owner_db,
        &storage,
        &owner,
        &owner,
        JoinerCancellationDisposition::Closure,
    )
    .await;
}

#[tokio::test]
async fn missing_joiner_writes_are_revoked_and_cleaned_up_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([46; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_post_attempt_cancellation(
        &owner_db,
        &storage,
        &owner,
        &member,
        JoinerCancellationDisposition::WriteRevocation,
    )
    .await;
}

#[tokio::test]
async fn missing_joiner_writes_are_revoked_and_cleaned_up_on_serial() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(
                run_missing_joiner_writes_are_revoked_and_cleaned_up_on_serial(),
            )
            .await
            .expect("Serial missing-joiner revocation orchestration");
        })
        .await;
}

async fn run_missing_joiner_writes_are_revoked_and_cleaned_up_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(&owner_db, "serial-device-join-revocation", owner.clone())
        .await
        .expect("create exact Serial Store");

    exercise_post_attempt_cancellation(
        &owner_db,
        &storage,
        &owner,
        &owner,
        JoinerCancellationDisposition::WriteRevocation,
    )
    .await;
}

#[tokio::test]
async fn missing_provider_administrator_writes_are_revoked_and_cleaned_up() {
    use crate::sync::membership::MemberRole;
    use crate::sync::storage::{
        ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, StoreProviderBinding,
    };

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = TestStore::create_with_provider_binding(
        &owner_db,
        "cross-principal-revocation-store",
        owner.clone(),
        ResolvedProviderBinding {
            store: StoreProviderBinding::Dropbox {
                namespace_id: "revocation-namespace".to_string(),
            },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::Dropbox {
                    account_id: "administrator-account".to_string(),
                },
            },
        },
    )
    .await
    .expect("create cross-principal test Store");
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([48; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");

    exercise_missing_provider_administrator(&owner_db, &storage, &owner, &member).await;
}

#[tokio::test]
async fn cancellation_removes_an_inflight_registration_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([52; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_cancellation_against_inflight_registration(&owner_db, &storage, &owner, &member).await;
}

#[tokio::test]
async fn cancellation_removes_an_inflight_registration_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(&owner_db, "serial-inflight-registration", owner.clone())
        .await
        .expect("create exact Serial Store");

    exercise_cancellation_against_inflight_registration(&owner_db, &storage, &owner, &owner).await;
}

#[tokio::test]
async fn provider_access_grant_create_resumes_after_pre_visibility_failure_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([49; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_provider_access_grant_create_interruption(
        &owner_db,
        &storage,
        &owner,
        &member,
        ExactCreateInterruption::BeforeVisibility,
    )
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_settles_lost_response_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key([50; 32]),
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");
    exercise_provider_access_grant_create_interruption(
        &owner_db,
        &storage,
        &owner,
        &member,
        ExactCreateInterruption::AfterVisibility,
    )
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_resumes_after_pre_visibility_failure_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(&owner_db, "serial-access-grant-resume", owner.clone())
        .await
        .expect("create exact Serial Store");
    exercise_provider_access_grant_create_interruption(
        &owner_db,
        &storage,
        &owner,
        &owner,
        ExactCreateInterruption::BeforeVisibility,
    )
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_settles_lost_response_on_serial() {
    let owner = UserKeypair::generate();
    let owner_db = open_serial_test_db();
    let storage = TestStore::create(
        &owner_db,
        "serial-access-grant-lost-response",
        owner.clone(),
    )
    .await
    .expect("create exact Serial Store");
    exercise_provider_access_grant_create_interruption(
        &owner_db,
        &storage,
        &owner,
        &owner,
        ExactCreateInterruption::AfterVisibility,
    )
    .await;
}

#[tokio::test]
async fn cross_principal_device_join_completes_on_the_runtime_stack() {
    use crate::sync::membership::MemberRole;
    use crate::sync::storage::{
        ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, StoreProviderBinding,
    };

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = TestStore::create_with_provider_binding(
        &owner_db,
        "cross-principal-test-store",
        owner.clone(),
        ResolvedProviderBinding {
            store: StoreProviderBinding::Dropbox {
                namespace_id: "shared-namespace".to_string(),
            },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::Dropbox {
                    account_id: "administrator-account".to_string(),
                },
            },
        },
    )
    .await
    .expect("create exact cross-principal test Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([43; 32]);
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact Member identity");

    let member_db = open_test_db();
    crate::sync::test_helpers::install_cross_principal_device_fixture(
        &storage,
        &owner_db,
        &member_db,
        &member,
        "member-account",
        T0,
    )
    .await
    .expect("complete cross-principal device join");

    assert!(
        member_db
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the cross-principal join activates the joining registration",
    );
}

/// The mirror of the above: an Owner device with local data and itself pinned as the
/// owner DOES author the snapshot — the founder/initial-sync path a freshly-founded
/// store bootstraps from is preserved by the gate's owner branch.
#[tokio::test]
async fn owner_device_creates_a_snapshot() {
    let owner = UserKeypair::generate();
    let db = open_test_db();
    let storage = cycle_test_store(&db, &owner).await;
    let (_tmp, ld) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [6u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &cipher, &owner, &hlc, &ld).await;

    assert!(
        latest_store_snapshot_meta(&db).await.is_some(),
        "an owner device must author catalog snapshot metadata",
    );
}
