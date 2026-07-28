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
use crate::clock::{FixedClock, SystemClock};
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
use crate::sync::store::StoreDatabase;
use crate::sync::store_commit::SnapshotMeta;
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct WriteRevocationRequest {
    producer: crate::sync::store::DeviceJoinProducer,
    authority: crate::sync::store::ProviderWriteAuthorityRef,
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
impl crate::sync::store::DeviceJoinWriteRevocationExecutor for ConfirmedWriteRevocation {
    async fn revoke_write_authority(
        &self,
        producer: crate::sync::store::DeviceJoinProducer,
        authority: &crate::sync::store::ProviderWriteAuthorityRef,
        locator: &crate::sync::provider::ProviderAccessLocator,
        protected_slots: &[crate::storage::cloud::ObjectSlot],
    ) -> Result<crate::sync::provider::ProviderAccessWithdrawal, crate::sync::store::DeviceJoinError>
    {
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(db),
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize test membership");
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
            &storage.storage,
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize membership");
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
    let Some((_reference, commit)) =
        load_exact_materialized_commit(db, &storage.storage, stream_id, sequence)
            .await
            .expect("load exact materialized Store commit")
    else {
        return false;
    };
    match crate::sync::store_objects::load_store_package(&storage.storage, &commit).await {
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
    let (registration_ref, registration) = crate::sync::store::StoreDatabase::new(db)
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
    crate::sync::store::push_store_snapshot(
        &storage.storage,
        storage.store_root_hash(),
        crate::sync::store::CreatedSnapshot {
            db_image: marker.to_vec(),
            blobs: Vec::new(),
        },
        crate::sync::store_commit::CommitFrontier(BTreeMap::new()),
        db.schema_version(),
        &storage.protocol_founder_keypair(),
        T0.to_string(),
        &membership,
        &crate::sync::store::StoreDatabase::new(db),
    )
    .await
    .expect("publish exact Store snapshot fixture");
}

async fn latest_store_snapshot_meta(db: &Database) -> Option<SnapshotMeta> {
    store_database(db)
        .latest_local_store_snapshot()
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
    owner_db: &'a crate::sync::store::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let authorization = Box::new(
            storage
                .open_into(owner_db.sqlite())
                .await
                .expect("load exact Store membership"),
        );
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::store::begin_device_join(
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
            Box::pin(crate::sync::store::prepare_device_provider_access_request(
                &pending,
                crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                    .await
                    .expect("resolve provider binding"),
                member,
                (*offer).clone(),
            ))
            .await
            .expect("prepare exact provider access request"),
        );
        let abandonment = Box::new(
            Box::pin(crate::sync::store::abandon_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                (*offer).clone(),
            ))
            .await
            .expect("abandon device join before attempt activation"),
        );
        let retried = Box::pin(crate::sync::store::abandon_device_join(
            owner_db,
            &storage.storage,
            &authorization,
            owner,
            *offer,
        ))
        .await
        .expect("retry device join abandonment");
        assert_eq!(retried, *abandonment);

        let observed = Box::pin(crate::sync::store::observe_device_join_abandonment(
            &pending,
            &storage.storage,
            &storage.root,
            (*abandonment).clone(),
        ))
        .await
        .expect("observe exact abandonment");
        let observed_retry = Box::pin(crate::sync::store::observe_device_join_abandonment(
            &pending,
            &storage.storage,
            &storage.root,
            (*abandonment).clone(),
        ))
        .await
        .expect("retry exact abandonment observation");
        assert_eq!(observed_retry, observed);
        assert!(matches!(
            crate::sync::store::load_store_device_join_status(
                owner_db.sqlite(),
                abandonment.abandonment.attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load owner join status"),
            Some(DeviceJoinStatus::Abandoned { abandonment: durable }) if durable == *abandonment
        ));
        assert!(matches!(
            crate::sync::store::load_pending_device_join_status(
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
    owner_db: &'a crate::sync::store::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    interruption: ExactCreateInterruption,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let authorization = Box::new(
            storage
                .open_into(owner_db.sqlite())
                .await
                .expect("load exact Store membership"),
        );
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::store::begin_device_join(
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
            Box::pin(crate::sync::store::prepare_device_provider_access_request(
                &pending,
                crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                    .await
                    .expect("resolve provider binding"),
                member,
                *offer,
            ))
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
        let first = Box::pin(crate::sync::store::authorize_device_provider_access(
            owner_db,
            &storage.storage,
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
                    crate::sync::store::load_store_device_join_status(
                        owner_db.sqlite(),
                        attempt_id,
                        DeviceJoinRole::ProviderAdministrator,
                    )
                    .await
                    .expect("load provider create status"),
                    Some(DeviceJoinStatus::ProviderAccessGrantCreatePending { .. })
                ));
                Box::pin(crate::sync::store::authorize_device_provider_access(
                    owner_db,
                    &storage.storage,
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
        let retry = Box::pin(crate::sync::store::authorize_device_provider_access(
            owner_db,
            &storage.storage,
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
            crate::sync::store::load_store_device_join_status(
                owner_db.sqlite(),
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
    owner_db: &'a crate::sync::store::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    joiner_disposition: JoinerCancellationDisposition,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{
            DeviceJoinRole, DeviceJoinStatus, JoinerJoinTerminal, ProviderAdminJoinTerminal,
        };

        let authorization = Box::new(
            storage
                .open_into(owner_db.sqlite())
                .await
                .expect("load exact Store membership"),
        );
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::store::begin_device_join(
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
            Box::pin(crate::sync::store::prepare_device_provider_access_request(
                &pending,
                crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                    .await
                    .expect("resolve provider binding"),
                member,
                *offer,
            ))
            .await
            .expect("prepare exact provider access request"),
        );
        let approval = Box::new(
            Box::pin(crate::sync::store::authorize_device_provider_access(
                owner_db,
                &storage.storage,
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
            Box::pin(crate::sync::store::prepare_device_registration_request(
                &pending,
                &storage.storage,
                None,
                member,
                *approval,
            ))
            .await
            .expect("prepare exact registration request"),
        );
        let provisional = Box::new(
            Box::pin(crate::sync::store::accept_device_registration_request(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                *registration_request,
            ))
            .await
            .expect("activate exact join attempt"),
        );
        let attempt_id = provisional.publication_authorization.attempt.attempt_id;
        let cancellation = Box::new(
            Box::pin(crate::sync::store::cancel_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel exact active join attempt"),
        );
        let cancellation_retry = Box::pin(crate::sync::store::cancel_device_join(
            owner_db,
            &storage.storage,
            &authorization,
            owner,
            provisional.publication_authorization.attempt.clone(),
        ))
        .await
        .expect("retry exact active join cancellation");
        assert_eq!(cancellation_retry, *cancellation);

        let administrator_terminal = Box::new(
            Box::pin(crate::sync::store::close_device_provider_admission(
                owner_db,
                &storage.storage,
                None,
                owner,
                (*cancellation).clone(),
            ))
            .await
            .expect("close exact provider admission"),
        );
        let administrator_retry = Box::pin(crate::sync::store::close_device_provider_admission(
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
                Box::pin(crate::sync::store::close_joining_device(
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
                Box::pin(crate::sync::store::revoke_joining_device_writes(
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
            if let crate::sync::store::DeviceProviderResponseReservation::CrossPrincipal {
                response_slot,
            } = &provisional.request.response
            {
                expected_slots.push(response_slot.clone());
            }
            assert_eq!(
                joiner_revocation.requests(),
                vec![WriteRevocationRequest {
                    producer: crate::sync::store::DeviceJoinProducer::Joiner,
                    authority: crate::sync::store::ProviderWriteAuthorityRef::MemberAccess(
                        provisional.request.approval.access_grant.grant_ref.clone(),
                    ),
                    locator: joiner_access_locator.clone(),
                    protected_slots: expected_slots,
                }],
            );
        }
        let joiner_retry = match joiner_disposition {
            JoinerCancellationDisposition::Closure => {
                Box::pin(crate::sync::store::close_joining_device(
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
                let terminal = Box::pin(crate::sync::store::revoke_joining_device_writes(
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
            crate::sync::store::load_store_device_join_actions(owner_db.sqlite())
                .await
                .expect("enumerate terminal Store join actions")
                .contains(
                    &crate::sync::store::DeviceJoinAction::TransferProviderAdminTerminal(
                        (*administrator_terminal).clone(),
                    ),
                )
        );
        let joiner_action = crate::sync::store::DeviceJoinAction::TransferJoinerTerminal(
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
                crate::sync::store::load_store_device_join_actions(owner_db.sqlite())
                    .await
                    .expect("enumerate replacement joiner terminal")
                    .contains(&joiner_action),
            ),
        }

        storage.home.fail_exact_create_before_call(1);
        let interrupted_cleanup = Box::pin(crate::sync::store::prepare_device_join_cleanup(
            owner_db,
            &storage.storage,
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
            crate::sync::store::load_store_device_join_status(
                owner_db.sqlite(),
                attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load interrupted cleanup status"),
            Some(DeviceJoinStatus::CleanupReceiptCreatePending { .. })
        ));
        let receipt = Box::new(
            Box::pin(crate::sync::store::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
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
        let receipt_retry = Box::pin(crate::sync::store::prepare_device_join_cleanup(
            owner_db,
            &storage.storage,
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
            Box::pin(crate::sync::store::activate_device_join_cleanup(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                attempt_id,
                (*receipt).clone(),
            ))
            .await
            .expect("activate exact cleanup receipt"),
        );
        let activation_retry = Box::pin(crate::sync::store::activate_device_join_cleanup(
            owner_db,
            &storage.storage,
            &authorization,
            owner,
            attempt_id,
            *receipt,
        ))
        .await
        .expect("retry exact cleanup activation");
        assert_eq!(activation_retry, *activation);

        let owner_complete = crate::sync::store::complete_owner_device_join_cleanup(
            owner_db,
            attempt_id,
            (*activation).clone(),
        )
        .await
        .expect("complete exact owner cleanup");
        let owner_complete_retry = crate::sync::store::complete_owner_device_join_cleanup(
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
            crate::sync::store::accept_joiner_device_join_cleanup(
                &pending,
                &storage.storage,
                &storage.root,
                forged_activation,
            )
            .await
            .is_err(),
            "joiner cleanup must reject an activation whose exact Store commit was not verified",
        );
        crate::sync::store::accept_joiner_device_join_cleanup(
            &pending,
            &storage.storage,
            &storage.root,
            (*activation).clone(),
        )
        .await
        .expect("accept exact joiner cleanup activation");
        let joiner_complete = crate::sync::store::complete_joiner_device_join_cleanup(
            &pending,
            (*activation).clone(),
        )
        .expect("complete exact joiner cleanup");
        let joiner_complete_retry =
            crate::sync::store::complete_joiner_device_join_cleanup(&pending, *activation)
                .expect("retry exact joiner cleanup completion");
        assert_eq!(joiner_complete_retry, joiner_complete);
        assert!(matches!(
            crate::sync::store::load_store_device_join_status(
                owner_db.sqlite(),
                attempt_id,
                DeviceJoinRole::Owner,
            )
            .await
            .expect("load owner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
        assert!(matches!(
            crate::sync::store::load_pending_device_join_status(&pending, attempt_id)
                .expect("load joiner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
    })
}

fn exercise_missing_provider_administrator<'a>(
    owner_db: &'a crate::sync::store::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::storage::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
        };
        use crate::sync::store::{JoinerJoinTerminal, ProviderAdminJoinTerminal};

        let authorization = Box::new(
            storage
                .open_into(owner_db.sqlite())
                .await
                .expect("load exact Store membership"),
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
        .expect("create peer exact storage");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::store::begin_device_join(
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
            Box::pin(crate::sync::store::prepare_device_provider_access_request(
                &pending,
                crate::sync::storage::SyncStorage::provider_binding(&peer_storage)
                    .await
                    .expect("resolve peer provider binding"),
                member,
                *offer,
            ))
            .await
            .expect("prepare cross-principal access request"),
        );
        let access_administrator = crate::sync::test_helpers::TestDropboxAccessAdministrator {
            namespace_id: namespace_id.clone(),
        };
        let approval = Box::new(
            Box::pin(crate::sync::store::authorize_device_provider_access(
                owner_db,
                &storage.storage,
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
            Box::pin(crate::sync::store::prepare_device_registration_request(
                &pending,
                &peer_storage,
                Some(peer_home.as_ref()),
                member,
                *approval,
            ))
            .await
            .expect("prepare cross-principal registration request"),
        );
        let provisional = Box::new(
            Box::pin(crate::sync::store::accept_device_registration_request(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                *registration_request,
            ))
            .await
            .expect("activate cross-principal join attempt"),
        );
        let attempt_id = provisional.publication_authorization.attempt.attempt_id;
        let cancellation = Box::new(
            Box::pin(crate::sync::store::cancel_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel cross-principal join attempt"),
        );
        owner_db
            .sqlite()
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
            Box::pin(crate::sync::store::revoke_device_provider_admission_writes(
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
            .expect("revoke absent provider-administrator writes"),
        );
        let crate::sync::store::DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
            &provisional.request.approval.admission
        else {
            panic!("missing-provider test did not create a cross-principal challenge");
        };
        assert_eq!(
            revocation.requests(),
            vec![WriteRevocationRequest {
                producer: crate::sync::store::DeviceJoinProducer::ProviderAdministrator,
                authority: crate::sync::store::ProviderWriteAuthorityRef::ProviderAdministrator(
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
        let administrator_retry =
            Box::pin(crate::sync::store::revoke_device_provider_admission_writes(
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
            ))
            .await
            .expect("retry provider-administrator write revocation");
        assert!(retry_revocation.requests().is_empty());
        assert_eq!(administrator_retry, *administrator_terminal);
        assert!(matches!(
            administrator_terminal.as_ref(),
            ProviderAdminJoinTerminal::WriteRevoked(_)
        ));
        let joiner_terminal = Box::new(
            Box::pin(crate::sync::store::close_joining_device(
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
            Box::pin(crate::sync::store::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
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
            Box::pin(crate::sync::store::activate_device_join_cleanup(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                attempt_id,
                *receipt,
            ))
            .await
            .expect("activate cleanup with revoked provider administrator"),
        );
        crate::sync::store::complete_owner_device_join_cleanup(
            owner_db,
            attempt_id,
            (*activation).clone(),
        )
        .await
        .expect("complete owner cleanup");
        crate::sync::store::accept_joiner_device_join_cleanup(
            &pending,
            &storage.storage,
            &storage.root,
            (*activation).clone(),
        )
        .await
        .expect("accept exact joiner cleanup activation");
        crate::sync::store::complete_joiner_device_join_cleanup(&pending, *activation)
            .expect("complete joiner cleanup");
    })
}

fn exercise_cancellation_against_inflight_registration<'a>(
    owner_db: &'a crate::sync::store::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        let authorization = Box::new(
            storage
                .open_into(owner_db.sqlite())
                .await
                .expect("load exact Store membership"),
        );
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = Box::new(
            Box::pin(crate::sync::store::begin_device_join(
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
            Box::pin(crate::sync::store::prepare_device_provider_access_request(
                &pending,
                crate::sync::storage::SyncStorage::provider_binding(&storage.storage)
                    .await
                    .expect("resolve provider binding"),
                member,
                *offer,
            ))
            .await
            .expect("prepare exact provider access request"),
        );
        let approval = Box::new(
            Box::pin(crate::sync::store::authorize_device_provider_access(
                owner_db,
                &storage.storage,
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
            Box::pin(crate::sync::store::prepare_device_registration_request(
                &pending,
                &storage.storage,
                None,
                member,
                *approval,
            ))
            .await
            .expect("prepare exact registration request"),
        );
        let provisional = Box::new(
            Box::pin(crate::sync::store::accept_device_registration_request(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                *registration_request,
            ))
            .await
            .expect("activate exact join attempt"),
        );
        let provider_ready = Box::new(
            Box::pin(crate::sync::store::publish_device_provider_challenge(
                owner_db,
                &storage.storage,
                None,
                (*provisional).clone(),
            ))
            .await
            .expect("publish same-principal provider readiness"),
        );
        let joining_db = open_test_db();
        storage
            .open_into(&joining_db)
            .await
            .expect("open exact Store for joining device");
        let (registration_visible, release_registration_create) =
            storage.home.pause_after_exact_create_call(1);
        let joining_database = crate::sync::store::StoreDatabase::new(&joining_db);
        let mut bootstrap = Box::pin(crate::sync::store::bootstrap_joining_device(
            &joining_database,
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
            Box::pin(crate::sync::store::cancel_device_join(
                owner_db,
                &storage.storage,
                &authorization,
                owner,
                provisional.publication_authorization.attempt.clone(),
            ))
            .await
            .expect("cancel while registration create is in flight"),
        );
        let administrator = Box::new(
            Box::pin(crate::sync::store::close_device_provider_admission(
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
            Box::pin(crate::sync::store::close_joining_device(
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
            Box::pin(crate::sync::store::prepare_device_join_cleanup(
                owner_db,
                &storage.storage,
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
        Box::pin(crate::sync::store::activate_device_join_cleanup(
            owner_db,
            &storage.storage,
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&db),
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&db),
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize test membership");

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
    let pending = store_database(&db)
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

    let published = store_database(&db)
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
    assert!(store_database(&db)
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
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &storage.storage,
        "test-lib",
        storage.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
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
            crate::blob::TransferLimits::one_at_a_time(),
            "restored-snapshot-device".to_string(),
            &test_migrations(),
            None,
            &storage.storage,
            &crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("install snapshot-only blob bootstrap");
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    assert_eq!(
        crate::sync::store::reconcile_snapshot_blobs(
            &crate::sync::store::StoreDatabase::new(&restored),
            &restore_path,
            &storage.storage,
            &restore_dir,
            &tables,
            &cancel_rx,
        )
        .await
        .expect("reconcile restored snapshot blob"),
        crate::sync::store::SnapshotBlobReconcile::Complete,
    );
    assert_eq!(
        crate::blob::cache::read_cached_exact(
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("load rejected snapshot publication")
        .is_none());
    assert!(store_database(&db)
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
            crate::blob::TransferLimits::one_at_a_time(),
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    let pending = store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("load cleanup snapshot")
        .expect("cleanup snapshot remains pending");
    let spool = pending.blobs[0]
        .spool_path
        .clone()
        .expect("cleanup snapshot has spool");
    store_database(&db)
        .complete_snapshot_publication(pending.reference)
        .await
        .expect("atomically complete snapshot and own spool cleanup");
    assert_eq!(
        store_database(&db)
            .snapshot_blob_spool_cleanup_paths()
            .await
            .expect("load durable cleanup"),
        vec![spool.clone()]
    );
    drop(db);

    let reopened = open();
    assert!(crate::sync::store::drain_outbound_store_snapshot(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(&reopened),
    )
    .await
    .expect("drain cleanup after restart")
    .is_none());
    assert!(!spool.exists());
    assert!(store_database(&reopened)
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("load rejected snapshot outbox")
        .is_none());
    assert!(store_database(&db)
        .snapshot_blob_spool_cleanup_paths()
        .await
        .expect("load rejected snapshot cleanup")
        .is_empty());
    assert!(store_database(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load rejected published snapshot")
        .is_none());

    let stored = create_exact_blob(&storage, "audio", "audio1", b"AUDIO").await;
    assert!(!store_dir
        .outbound_blob_spool_path(stored.locator().locator_hash())
        .exists());
    let (registration_ref, registration) = crate::sync::store::StoreDatabase::new(&db)
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
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("load completed snapshot outbox")
        .is_none());
    assert!(store_database(&db)
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
async fn owner_membership_anchor_founds_pins_and_refuses_tampering() {
    use crate::sync::store::anchor_owner_membership;
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(&db, "test-store", owner.clone())
        .await
        .expect("create exact Store");

    anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    let membership =
        crate::sync::store::load_cycle_membership(&storage.storage, &store_database(&db))
            .await
            .expect("load exact founder membership");
    assert!(
        membership.is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
        &storage.root,
        storage.protocol_root(),
        &owner,
    )
    .await
    .expect("re-connect anchors to the pinned owner");
    let owner_before = db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let graph = store_database(&db)
        .local_store_founder_graph()
        .await
        .expect("read founder graph")
        .expect("founder graph exists");
    let crate::database::DurableFounderMembership { head, .. } = graph.membership;
    storage
        .storage
        .delete_protocol_object(&head.object)
        .await
        .expect("delete exact founder head");
    assert!(
        anchor_owner_membership(
            &storage.storage,
            &store_database(&db),
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
    let root = crate::sync::store::protocol_root::open_store(
        &store_database(&opened_db),
        &storage.storage,
        &storage.root,
    )
    .await
    .expect("open exact Store root");
    assert_eq!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read founder device genesis before anchoring"),
        None,
    );

    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&opened_db),
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
    use crate::sync::store::anchor_owner_membership;
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(&db, "test-store", owner.clone())
        .await
        .expect("create exact Store");
    db.delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove local owner pin");
    anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
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
    let foreign_root = crate::sync::store::protocol_root::open_store(
        &store_database(&fresh_db),
        &seeded.storage,
        &seeded.root,
    )
    .await
    .expect("open the pinned foreign Store root");
    assert!(
        anchor_owner_membership(
            &seeded.storage,
            &store_database(&fresh_db),
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
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

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

    let components = cycle::init_sync_over_storage(
        &crate::sync::store::StoreDatabase::new(&db),
        storage,
        cycle::StoreInitialization::CreateStore,
        None,
    )
    .await
    .expect("initialize plaintext storage");

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let root = store_database(&db)
        .local_store_root_ref()
        .await
        .unwrap()
        .expect("initialization persists the exact Store root");
    let protocol_root =
        crate::sync::store_objects::load_store_protocol_root(components.storage().as_ref(), &root)
            .await
            .expect("open exact Store root")
            .value;
    let membership = crate::sync::store::load_cycle_membership(
        components.storage().as_ref(),
        &store_database(&db),
    )
    .await
    .expect("load exact founder membership");
    protocol_root
        .descriptor
        .validate_merge_founder_entry(
            membership
                .entries()
                .first()
                .expect("Store has a founder entry"),
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
async fn initialization_refuses_a_founder_entry_without_its_store_protocol_root() {
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

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
        &crate::sync::store::StoreDatabase::new(&db),
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
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

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
        &crate::sync::store::StoreDatabase::new(&db),
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
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

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
        &crate::sync::store::StoreDatabase::new(&db),
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
    use crate::sync::store::OWNER_PUBKEY_STATE_KEY;

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
            &crate::sync::store::StoreDatabase::new(&db),
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
                &crate::sync::store::StoreDatabase::new(&db),
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
    storage: crate::sync::test_helpers::InterceptedStorage<
        Arc<CloudSyncStorage>,
        CycleStorageInterception,
    >,
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
        reject_create_call: Option<usize>,
        reject_prepare_call: Option<usize>,
        allocate_calls: AtomicUsize,
        prepare_calls: AtomicUsize,
        create_calls: AtomicUsize,
        attempted: std::sync::Mutex<Vec<crate::blob::locator::StoredBlobRef>>,
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

    fn inject_host_write(inner: TestStore, db: Database, write_sql: &str) -> Self {
        Self::new(
            Arc::new(inner),
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
                reject_create_call: Some(reject_call),
                reject_prepare_call: None,
                allocate_calls: AtomicUsize::new(0),
                prepare_calls: AtomicUsize::new(0),
                create_calls: AtomicUsize::new(0),
                attempted: std::sync::Mutex::new(Vec::new()),
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn reject_blob_prepare(inner: Arc<TestStore>) -> Self {
        Self::new(
            inner,
            CycleStorageInterception::RejectBlobCreate {
                reject_create_call: None,
                reject_prepare_call: Some(1),
                allocate_calls: AtomicUsize::new(0),
                prepare_calls: AtomicUsize::new(0),
                create_calls: AtomicUsize::new(0),
                attempted: std::sync::Mutex::new(Vec::new()),
                protocol_read_calls: AtomicUsize::new(0),
            },
        )
    }

    fn new(inner: Arc<TestStore>, interceptor: CycleStorageInterception) -> Self {
        Self {
            storage: crate::sync::test_helpers::InterceptedStorage::new(
                Arc::clone(&inner.storage),
                interceptor,
            ),
            inner,
        }
    }

    fn rejected_blobs(&self) -> Vec<crate::blob::locator::StoredBlobRef> {
        self.storage.interceptor().rejected_blobs()
    }

    fn blob_write_calls(&self) -> (usize, usize, usize) {
        self.storage.interceptor().blob_write_calls()
    }
}

impl std::ops::Deref for CycleStorageInterceptor {
    type Target = crate::sync::test_helpers::InterceptedStorage<
        Arc<CloudSyncStorage>,
        CycleStorageInterception,
    >;

    fn deref(&self) -> &Self::Target {
        &self.storage
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

    fn rejected_blobs(&self) -> Vec<crate::blob::locator::StoredBlobRef> {
        let Self::RejectBlobCreate { attempted, .. } = self else {
            panic!("storage interception does not reject blob creates");
        };
        attempted
            .lock()
            .expect("attempted blob record lock")
            .clone()
    }

    fn blob_write_calls(&self) -> (usize, usize, usize) {
        let Self::RejectBlobCreate {
            allocate_calls,
            prepare_calls,
            create_calls,
            ..
        } = self
        else {
            panic!("storage interception does not record blob writes");
        };
        (
            allocate_calls.load(Ordering::SeqCst),
            prepare_calls.load(Ordering::SeqCst),
            create_calls.load(Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl crate::sync::test_helpers::StorageInterceptor for CycleStorageInterception {
    async fn before_protocol_create(
        &self,
        prepared: &crate::sync::storage::PreparedExactObject,
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
                    host_exec(db, write_sql).await;
                }
            }
        }
        Ok(())
    }

    async fn before_blob_allocate(&self) -> Result<(), StorageError> {
        if let Self::RejectBlobCreate { allocate_calls, .. } = self {
            allocate_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn before_blob_prepare(&self) -> Result<(), StorageError> {
        if let Self::RejectBlobCreate {
            reject_prepare_call,
            prepare_calls,
            ..
        } = self
        {
            let call = prepare_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if *reject_prepare_call == Some(call) {
                return Err(StorageError::Storage(format!(
                    "unexpected blob prepare call {call}"
                )));
            }
        }
        Ok(())
    }

    async fn before_blob_create(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
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
            if *reject_create_call == Some(call) {
                return Err(StorageError::Storage(format!(
                    "unexpected blob create call {call}"
                )));
            }
        }
        Ok(())
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
                let peer_sequence = crate::sync::store::StoreDatabase::new(&producer_db)
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
    let peer_sequence = crate::sync::store::StoreDatabase::new(&producer_db)
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
    let before = crate::sync::store::StoreDatabase::new(&db_m)
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
    let after = crate::sync::store::StoreDatabase::new(&db_m)
        .latest_local_store_position()
        .await
        .expect("read local Store position after the empty cycle")
        .expect("the empty cycle publishes its acknowledgement");
    let (_, registration) = crate::sync::store::StoreDatabase::new(&db_m)
        .local_blob_write_authority()
        .await
        .expect("load local Store registration");
    let mut commit_verifier =
        crate::sync::store::StoreCommitVerifier::new(&storage.storage, &storage.root)
            .await
            .expect("open Store commit verifier");
    let commit = commit_verifier
        .load_ref(&after)
        .await
        .expect("load empty-cycle acknowledgement commit");
    assert_eq!(commit.author(), &registration);
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
    let prepared = crate::sync::store::StoreDatabase::new(&db)
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
        let (_commit_ref, commit) =
            load_exact_materialized_commit(&db, &storage.storage, &stream_id, seq)
                .await
                .expect("load exact materialized commit")
                .expect("write has a commit");
        let package = crate::sync::store_objects::load_store_package(&storage.storage, &commit)
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
        .write_id
        .clone();

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
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("adopted Store write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(
        crate::sync::store::StoreDatabase::new(&db)
            .exact_materialized_ref(&stream_id, published.coord.sequence())
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
    crate::blob::transition::make_remote(
        &crate::sync::store::StoreDatabase::new(&db),
        &ld,
        hlc.as_ref(),
        "notes",
        "n1",
        false,
    )
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
    crate::blob::transition::make_remote(
        &crate::sync::store::StoreDatabase::new(&db),
        &ld,
        &hlc,
        "notes",
        "transport-root",
        false,
    )
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
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
    let prepared = crate::sync::store::StoreDatabase::new(&db)
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
            crate::PublishedPosition {
                device_id: published_device,
                commit,
            } if published_device == device_id => commit,
            position => panic!("retried Store write has wrong position: {position:?}"),
        },
        status => panic!("retried Store write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    let published = crate::sync::store::StoreDatabase::new(&db)
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
    assert_eq!(published_commit.write_id, write_id);
    assert!(
        published_commit.store_package().is_some(),
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
    let published = store_database(db)
        .latest_local_store_ack()
        .await
        .expect("read latest exact Store acknowledgement")
        .expect("the cycle published an acknowledgement");
    let root = store_database(db)
        .local_store_root_ref()
        .await
        .expect("read exact Store root")
        .expect("exact Store root exists");
    let local_device = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registration = crate::sync::store::StoreDatabase::new(db)
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
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("push-timestamp write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
        let local_at_snapshot = crate::sync::store::StoreDatabase::new(&db)
            .latest_local_store_position()
            .await
            .expect("read local Store position after peer setup")
            .expect("local Store stream has an exact snapshot position");
        let local_snapshot_sequence = local_at_snapshot.coord.sequence();
        let local_stream = local_at_snapshot.coord.stream_id;
        let peer_stream = peer_at_snapshot.coord.stream_id;
        let membership = storage
            .open_into(&db)
            .await
            .expect("open Store before publishing cadence snapshot");
        crate::sync::store::push_store_snapshot(
            &storage.storage,
            storage.store_root_hash(),
            crate::sync::store::CreatedSnapshot {
                db_image: b"cadence-snapshot".to_vec(),
                blobs: Vec::new(),
            },
            crate::sync::store_commit::CommitFrontier(BTreeMap::from([
                (local_stream, local_at_snapshot),
                (peer_stream, peer_at_snapshot),
            ])),
            db.schema_version(),
            &owner,
            T0.to_string(),
            &membership,
            &crate::sync::store::StoreDatabase::new(&db),
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
            crate::sync::store::StoreDatabase::new(&db)
                .latest_local_store_position()
                .await
                .expect("read latest local Store commit")
                .expect("local Store stream has commits")
                .coord
                .sequence(),
            local_after_snapshot,
        );

        let unregistered_member = UserKeypair::generate();
        crate::sync::store::invite_member(
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
            &crate::sync::store::StoreDatabase::new(&db),
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
            store_database(&db)
                .latest_local_store_snapshot()
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

#[tokio::test]
async fn snapshot_time_cadence_uses_the_signed_snapshot_timestamp() {
    tokio::spawn(async {
        let owner = UserKeypair::generate();
        let db = open_test_db();
        let storage = cycle_test_store(&db, &owner).await;
        let source = open_test_db();
        let first = capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-1', 'First', NULL, 1, \
                         '0000000001000-0000-source', '2026-01-01')",
            ],
        )
        .await;
        let second = capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-2', 'Second', NULL, 1, \
                         '0000000002000-0000-source', '2026-01-01')",
            ],
        )
        .await;

        let at_snapshot = storage
            .publish_changeset("local", 1, &first, SCHEMA_VERSION)
            .await
            .expect("publish Store commit before snapshot");
        let membership = storage
            .open_into(&db)
            .await
            .expect("open Store before publishing timed snapshot");
        crate::sync::store::push_store_snapshot(
            &storage.storage,
            storage.store_root_hash(),
            crate::sync::store::CreatedSnapshot {
                db_image: b"time-cadence-snapshot".to_vec(),
                blobs: Vec::new(),
            },
            crate::sync::store_commit::CommitFrontier(BTreeMap::from([(
                at_snapshot.coord.stream_id,
                at_snapshot,
            )])),
            db.schema_version(),
            &owner,
            T0.to_string(),
            &membership,
            &crate::sync::store::StoreDatabase::new(&db),
        )
        .await
        .expect("publish timed snapshot");
        storage
            .publish_changeset("local", 2, &second, SCHEMA_VERSION)
            .await
            .expect("publish one Store commit after snapshot");

        crate::sync::store::anchor_owner_membership(
            &storage.storage,
            &store_database(&db),
            &storage.root,
            storage.protocol_root(),
            &storage.protocol_founder_keypair(),
        )
        .await
        .expect("initialize timed snapshot membership");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read timed snapshot device")
            .expect("timed snapshot device exists");
        let (_temp, store_dir) = temp_store_dir();
        let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
            [25u8; 32],
        )));
        let now = chrono::DateTime::parse_from_rfc3339("2024-01-02T01:00:00Z")
            .expect("parse timed snapshot clock")
            .with_timezone(&chrono::Utc);
        run_single_sync_cycle(
            &storage.storage,
            &device_id,
            &Hlc::new("local".to_string()),
            &FixedClock(now),
            &db,
            &cipher,
            &PendingRotation::none(),
            &owner,
            None,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("run timed snapshot cycle");

        assert_eq!(
            store_database(&db)
                .latest_local_store_snapshot()
                .await
                .expect("read timed Store snapshot")
                .expect("time cadence publishes another snapshot")
                .reference
                .generation,
            1,
        );
    })
    .await
    .expect("snapshot time cadence orchestration completes");
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
        crate::sync::store::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
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
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
    crate::sync::store::anchor_owner_membership(
        &storage.storage,
        &store_database(&db),
        &storage.root,
        storage.protocol_root(),
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("initialize test membership");
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
    let first_write_id = crate::sync::store::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write")
        .expect("the exact Store write remains after append failure")
        .commit
        .value
        .write_id
        .clone();
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
                    crate::PublishedPosition { commit, .. }
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
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("retried preparation write is not published: {status:?}"),
    };
    let stream_id = local_store_stream_id(&db).await;
    assert!(crate::sync::store::StoreDatabase::new(&db)
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
    crate::sync::store::anchor_owner_membership(
        &storage.inner.storage,
        &store_database(db),
        &storage.inner.root,
        storage.inner.protocol_root(),
        &storage.inner.protocol_founder_keypair(),
    )
    .await
    .expect("initialize test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact test device id")
        .expect("exact test device id exists");
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
    let published_stream = published.coord.stream_id;
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
        crate::sync::store_commit::CommitFrontier(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    let ack_ref = store_database(&db_m)
        .latest_local_store_ack()
        .await
        .expect("read reclamation acknowledgement")
        .expect("the reclamation cycle publishes an acknowledgement")
        .reference;
    let root = store_database(&db_m)
        .local_store_root_ref()
        .await
        .expect("read reclamation Store root")
        .expect("reclamation Store root exists");
    let local_device = db_m
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registrations = crate::sync::store::StoreDatabase::new(&db_m)
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
        crate::sync::store_commit::StoreHistoryCut(frontier)
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact behind Member identity");
    let behind_db = open_test_db();
    activate_joined_test_device(&storage, &owner_db, &behind_db, &behind).await;
    pull_into(&behind_db, &storage, &ld).await;

    let behind_frontier = crate::sync::store_commit::CommitFrontier::from_refs(
        crate::sync::store::StoreDatabase::new(&behind_db)
            .materialized_frontier()
            .await
            .expect("read behind device frontier"),
    )
    .expect("validate behind device frontier");
    crate::sync::store::stage_store_acknowledgement_for_test(
        &behind_db,
        &storage.storage,
        behind_frontier,
        T0.to_string(),
        &behind,
    )
    .await
    .expect("stage behind device acknowledgement");
    crate::sync::store::drain_store_acknowledgements_for_test(
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
    let second_sequence = crate::sync::store::StoreDatabase::new(&owner_db)
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
        let (owner_registration, registration) = crate::sync::store::StoreDatabase::new(&owner_db)
            .local_blob_write_authority()
            .await
            .expect("read owner announcement authority");
        let owner_stream = registration
            .store_announcement_activation(&owner_registration)
            .expect("derive owner Store announcement activation")
            .author_stream_id()
            .to_string();
        let second_sequence = crate::sync::store::StoreDatabase::new(&owner_db)
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
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
async fn pull_refreshes_snapshot_authority_before_publication() {
    use crate::sync::membership::MemberRole;

    let founder = UserKeypair::generate();
    let founder_db = open_test_db();
    let storage = cycle_test_store(&founder_db, &founder).await;
    let successor_owner = UserKeypair::generate();
    let encryption = EncryptionService::from_key([64; 32]);
    crate::sync::store::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &founder,
        &Hlc::new("founder".to_string()),
        &pubkey_hex(&successor_owner),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &StoreDatabase::new(&founder_db),
    )
    .await
    .expect("invite successor Owner");
    let successor_db = open_test_db();
    install_active_device_fixture(
        &storage,
        &founder_db,
        &successor_db,
        &successor_owner,
        "2026-07-26T00:00:00Z",
    )
    .await
    .expect("activate successor Owner device");
    promote_active_member_fixture(
        &storage,
        &founder_db,
        &successor_db,
        &founder,
        &successor_owner,
        &encryption,
    )
    .await
    .expect("promote successor Owner");

    let founder_store = storage
        .loaded_store(&founder_db)
        .await
        .expect("load founder Store");
    let mut authorized = founder_store
        .authorize()
        .await
        .expect("authorize founder before removal");

    let custody = TestCustody::default();
    let cipher = RwLock::new(CloudCipher::Encrypted(encryption.clone()));
    crate::sync::store::remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &successor_owner,
        &Hlc::new("successor-owner".to_string()),
        &pubkey_hex(&founder),
        &encryption,
        &custody,
        &cipher,
        &PendingRotation::none(),
        &StoreDatabase::new(&successor_db),
    )
    .await
    .expect("remove founder after cycle authorization");

    let (_temp, store_dir) = temp_store_dir();
    let founder_device_id = founder_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load founder device id")
        .expect("active founder device id");
    authorized
        .pull(&store_dir, &founder, Some(&encryption))
        .await
        .expect("pull founder removal");
    authorized
        .publish_due_snapshots(
            &founder_device_id,
            &store_dir,
            &founder,
            "2026-07-26T01:00:00Z",
            Some(&encryption),
            false,
        )
        .await
        .expect("evaluate snapshot after pull");

    assert!(
        StoreDatabase::new(&founder_db)
            .latest_local_store_snapshot()
            .await
            .expect("read founder snapshot state")
            .is_none(),
        "a removed Owner must not publish from pre-pull membership authority",
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");

    let member_db = open_test_db();
    activate_joined_test_device(&storage, &owner_db, &member_db, &member).await;

    assert!(
        crate::sync::store::StoreDatabase::new(&member_db)
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the public join sequence activates the joining registration",
    );
}

struct SamePrincipalApprovalFixture {
    _pending_dir: tempfile::TempDir,
    pending: crate::sync::store::DeviceJoinJournalDatabase,
    authorization: crate::sync::membership::MembershipChain,
    approval: crate::sync::store::DeviceProviderAdmissionApproval,
}

async fn prepare_same_principal_approval_fixture(
    owner_db: &Database,
    storage: &TestStore,
    owner: &UserKeypair,
    member: &UserKeypair,
    hlc_node: &str,
) -> SamePrincipalApprovalFixture {
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(owner_db),
    )
    .await
    .expect("invite exact Member identity");
    let authorization = storage
        .open_into(owner_db)
        .await
        .expect("load exact Store membership");
    let pending_dir = tempfile::tempdir().expect("create join directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
        pending_dir.path().join("pending.sqlite"),
    )
    .expect("open join journal");
    let offer = crate::sync::store::begin_device_join(
        &crate::sync::store::StoreDatabase::new(owner_db),
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
    let access_request = crate::sync::store::prepare_device_provider_access_request(
        &pending,
        SyncStorage::provider_binding(&storage.storage)
            .await
            .expect("resolve provider binding"),
        member,
        offer,
    )
    .await
    .expect("prepare exact provider request");
    let approval = crate::sync::store::authorize_device_provider_access(
        &crate::sync::store::StoreDatabase::new(owner_db),
        &storage.storage,
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
    let first_registration_request = crate::sync::store::prepare_device_registration_request(
        &first.pending,
        &storage.storage,
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

    crate::sync::store::accept_device_registration_request(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage.storage,
        &second.authorization,
        &owner,
        first_registration_request,
    )
    .await
    .expect("the later predecessor head covers the first access activation");
}

/// The owner's registration-acceptance step is single-shot, and it is the
/// device's own sync loop it has to survive.
///
/// The step composes the join attempt against one exact position on the owner's
/// own Store stream: the attempt carries that position as its `bootstrap_cut`,
/// and pull refuses an attempt whose cut is not its commit's predecessor cut.
/// The attempt object is created once, at the slot the offer signed, so a new
/// position would need a new attempt body the offer will not admit. The step
/// also advances its journal past `Offered` before that commit activates, and
/// the state it lands in carries no prepared object to resume from — so a step
/// that loses its position leaves the owner with no way forward and the joining
/// device waiting on an artifact that is never coming.
///
/// The same device runs a sync loop that publishes its queued writes at that
/// very position. The step cannot lose to it: the turn to author this device's
/// own stream is taken when the position is read, travels inside the plan, and
/// is released only after the head that takes the position is published. The
/// drain waits for that turn, and a queued write that finds its position taken
/// re-prepares against the winner — it is the one composer that can lose a
/// position safely.
#[tokio::test]
async fn registration_acceptance_holds_its_position_against_the_owners_own_sync_loop() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = Arc::new(cycle_test_store(&owner_db, &owner).await);
    let member = UserKeypair::generate();
    let approval = prepare_same_principal_approval_fixture(
        &owner_db,
        &storage,
        &owner,
        &member,
        "contended-join-member",
    )
    .await;
    let request = crate::sync::store::prepare_device_registration_request(
        &approval.pending,
        &storage.storage,
        None,
        &member,
        approval.approval,
    )
    .await
    .expect("prepare the joining device's registration request");

    // A row of the owner's own, queued as a Store write: the sync loop now
    // holds a head addressed to the same position the acceptance composes
    // against.
    host_exec(
        &owner_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('queued', 'queued by the owner', NULL, 1, \
         '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    let device_id = owner_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read the owner's device id")
        .expect("the owner's store has an activated local device");
    let (_write_dir, store_dir) = temp_store_dir();
    {
        let writer = crate::sync::store::Store::authorize_borrowed(&storage.storage, &owner_db)
            .await
            .expect("authorize the owner's Store writer");
        assert!(
            writer
                .prepare_pending_store_write(&device_id, T0, &owner, &store_dir)
                .await
                .expect("prepare the owner's queued Store write"),
            "the owner's row queues a Store write for the sync loop to publish",
        );
    }

    let mut test_points = owner_db.observe_test_points();
    let (position_held, resume_acceptance) =
        owner_db.arm_test_pause(crate::database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld);
    let accept_db = owner_db.clone();
    let accept_storage = storage.clone();
    let accept_authorization = approval.authorization.clone();
    let accept_owner = owner.clone();
    let acceptance = tokio::spawn(async move {
        Box::pin(crate::sync::store::accept_device_registration_request(
            &crate::sync::store::StoreDatabase::new(&accept_db),
            &accept_storage.storage,
            &accept_authorization,
            &accept_owner,
            request,
        ))
        .await
    });

    // Hold the acceptance exactly where it has read the position and not yet
    // published the head that takes it — the window the sync loop used to
    // publish into.
    position_held.notified().await;
    let drain_db = owner_db.clone();
    let drain_storage = storage.clone();
    let drain = tokio::spawn(async move {
        let writer =
            crate::sync::store::Store::authorize_borrowed(&drain_storage.storage, &drain_db)
                .await
                .expect("authorize the owner's Store writer");
        Box::pin(writer.drain_store_writes()).await
    });
    // Uploading its commit is the step immediately before the drain would create
    // the head that takes the position. While the acceptance holds that
    // position, the drain must not get that far — it is still waiting for the
    // turn. Reaching it means the position was taken out from under a step that
    // cannot survive losing it.
    let reached_the_position = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(point) = test_points.recv().await {
            if matches!(
                point,
                crate::database::DatabaseTestPoint::StoreWriteCommitUploaded { .. }
            ) {
                return true;
            }
        }
        false
    })
    .await;
    assert!(
        reached_the_position.is_err(),
        "the sync loop reached the acceptance's position while the acceptance held it",
    );
    resume_acceptance.notify_one();

    let accepted = acceptance
        .await
        .expect("join the acceptance task")
        .expect("the acceptance keeps the position it composed against");
    let drained = drain
        .await
        .expect("join the sync loop drain task")
        .expect("the sync loop resolves losing the position");
    assert_eq!(
        accepted
            .publication_authorization
            .attempt_activation
            .coord
            .sequence(),
        2,
        "the acceptance activates at the position it read, not one past it",
    );
    assert_eq!(
        drained, 0,
        "the queued write found its position taken and re-prepares instead",
    );
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
    let request = crate::sync::store::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
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
    let plan = crate::sync::store::prepare_store_operation_plan_for_test(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage.storage,
        &fixture.authorization,
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
    let mut history_verifier =
        crate::sync::store::MergeHistoryVerifier::new(&storage.storage, &offer.store_root)
            .await
            .expect("open Store history verifier");
    crate::sync::store::load_verified_device_join_attempt(
        &mut history_verifier,
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
    let valid_request = crate::sync::store::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
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
        crate::sync::store::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
            valid_request.approval.request.as_ref().clone(),
            invalid_access,
            valid_request.approval.admission.clone(),
            &storage
                .founder_device_authority()
                .await
                .expect("load exact founder authority")
                .2,
        );
    let malformed_request = crate::sync::store::DeviceRegistrationRequest::signed(
        malformed_approval,
        valid_request.expected_registration.clone(),
        valid_request.registration_slot.clone(),
        valid_request.response.clone(),
        &member,
    )
    .expect("joiner signs malformed remote request fixture");
    crate::sync::store::accept_device_registration_request(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage.storage,
        &fixture.authorization,
        &owner,
        malformed_request,
    )
    .await
    .expect_err("Owner rejects the absent exact provider-access activation");
    crate::sync::store::accept_device_registration_request(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage.storage,
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
        crate::sync::store::invite_member(
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
            &crate::sync::store::StoreDatabase::new(&founder_db),
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
        let excluding_store = storage
            .loaded_store(&excluding_db)
            .await
            .expect("load excluding Owner Store");
        let proposal = match excluding_store
            .propose_device_exclusion(&excluding_owner, &founder_registration)
            .await
            .expect("propose founder device exclusion")
        {
            crate::sync::store::StoreDeviceExclusionResult::ProposalActivated {
                proposal, ..
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
            crate::sync::store::StoreDatabase::new(&excluding_db)
                .materialized_frontier()
                .await
                .expect("load exclusion frontier"),
        )
        .expect("shape exclusion frontier");
        crate::sync::store::stage_store_acknowledgement_for_test(
            &excluding_db,
            &storage.storage,
            frontier,
            "2026-07-20T00:01:00Z".to_string(),
            &excluding_owner,
        )
        .await
        .expect("stage exclusion acknowledgement");
        crate::sync::store::drain_store_acknowledgements_for_test(
            &excluding_db,
            &storage.storage,
            &excluding_owner,
        )
        .await
        .expect("publish exclusion acknowledgement");
        match excluding_store
            .finalize_device_exclusion(&excluding_owner, &proposal)
            .await
            .expect("activate founder exclusion")
        {
            crate::sync::store::StoreDeviceExclusionResult::OutcomeActivated { .. } => {}
            result => panic!("unexpected exclusion outcome result: {result:?}"),
        }

        crate::sync::store::prepare_device_registration_request(
            &approval.pending,
            &storage.storage,
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

    crate::sync::store::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
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
    let (next_slot, _) = crate::sync::store::exact_next_announcement_slot_for_test(
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
    crate::sync::store::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
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
    let (next_slot, accepted_head_ref) = crate::sync::store::exact_next_announcement_slot_for_test(
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
    let stream_id = activation.coord.stream_id;
    let mut next_commit = activation;
    next_commit.coord = crate::sync::store_commit::StoreCommitCoord {
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

    crate::sync::store::prepare_device_registration_request(
        &fixture.pending,
        &storage.storage,
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_pre_attempt_abandonment(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn post_attempt_device_join_cancellation_closes_and_cleans_up_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_post_attempt_cancellation(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_post_attempt_cancellation(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");

    exercise_missing_provider_administrator(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn cancellation_removes_an_inflight_registration_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_cancellation_against_inflight_registration(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_resumes_after_pre_visibility_failure_on_merge() {
    use crate::sync::membership::MemberRole;

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(&owner_db, &owner).await;
    let member = UserKeypair::generate();
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_provider_access_grant_create_interruption(
        &crate::sync::store::StoreDatabase::new(&owner_db),
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite exact Member identity");
    exercise_provider_access_grant_create_interruption(
        &crate::sync::store::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
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
    crate::sync::store::invite_member(
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
        &crate::sync::store::StoreDatabase::new(&owner_db),
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
        crate::sync::store::StoreDatabase::new(&member_db)
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
