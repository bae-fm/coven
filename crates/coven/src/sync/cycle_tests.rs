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
use std::sync::{Arc, Mutex};

use crate::database::Database;
use crate::database::StoreDatabase;
use crate::storage::cloud::{test_utils::InMemoryCloudHome, CloudHome};
use crate::storage::SyncStorage;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle;
use crate::sync::test_helpers::*;
use coven_foundation::clock::{FixedClock, SystemClock};
use coven_foundation::store_dir::StoreDir;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::store_commit::SnapshotMeta;
use coven_protocol::synced_schema::{BlobDecl, SyncedTable};

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct WriteRevocationRequest {
    producer: coven_protocol::store_commit::device_join_exchange::DeviceJoinProducer,
    authority: coven_protocol::store_commit::device_join_exchange::ProviderWriteAuthorityRef,
    locator: coven_protocol::provider::ProviderAccessLocator,
    protected_slots: Vec<coven_protocol::objects::ObjectSlot>,
}

struct ConfirmedWriteRevocation {
    withdrawal: coven_protocol::provider::ProviderAccessWithdrawal,
    requests: Mutex<Vec<WriteRevocationRequest>>,
}

impl ConfirmedWriteRevocation {
    fn direct(locator: coven_protocol::provider::ProviderAccessLocator) -> Self {
        Self {
            withdrawal: coven_protocol::provider::ProviderAccessWithdrawal::Direct {
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
        producer: coven_protocol::store_commit::device_join_exchange::DeviceJoinProducer,
        authority: &coven_protocol::store_commit::device_join_exchange::ProviderWriteAuthorityRef,
        locator: &coven_protocol::provider::ProviderAccessLocator,
        protected_slots: &[coven_protocol::objects::ObjectSlot],
    ) -> Result<
        coven_protocol::provider::ProviderAccessWithdrawal,
        crate::sync::store::DeviceJoinError,
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

async fn cycle_test_store(
    db: &Database,
    signer: &UserKeypair,
    home: Arc<crate::InMemoryCloudHome>,
) -> std::sync::Arc<TestStore> {
    TestStore::create(db, "test-lib", signer.clone(), home)
        .await
        .expect("create exact cycle test Store")
}

/// A fresh owner Store plus the second identity its device-join cases admit.
struct OwnerAndMember {
    owner: UserKeypair,
    owner_db: Database,
    storage: Arc<TestStore>,
    member: UserKeypair,
}

async fn owner_and_member() -> OwnerAndMember {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = cycle_test_store(
        &owner_db,
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    OwnerAndMember {
        owner,
        owner_db,
        storage,
        member: UserKeypair::generate(),
    }
}

/// Invites `member` into the owner's Store as an ordinary member.
async fn invite_test_member(
    storage: &TestStore,
    owner_db: &Database,
    owner: &UserKeypair,
    member: &UserKeypair,
    encryption: &EncryptionService,
) {
    storage
        .invite_member(
            owner_db,
            owner,
            &pubkey_hex(member),
            None,
            coven_protocol::membership::MemberRole::Member,
            encryption,
            "Test Store",
        )
        .await
        .expect("invite exact Member identity");
}

/// A `note_photos` schema whose rows carry a blob at `fill`, plus the Store its
/// owner publishes through.
async fn blob_cycle_store(keypair: &UserKeypair, fill: CacheFill) -> (Database, Arc<TestStore>) {
    let db = open_test_db_with_blob(BlobDecl::new("photos", Provenance::HostProvided, fill));
    let storage =
        cycle_test_store(&db, keypair, crate::sync::test_helpers::test_cloud_home()).await;
    (db, storage)
}

async fn run_cycle_in_task(
    storage: Arc<CycleStorageInterceptor>,
    device: TestDevice,
    store_dir: StoreDir,
) -> Result<(), cycle::SyncCycleFailure> {
    tokio::spawn(async move {
        storage
            .run_sync_cycle(&device, &store_dir)
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
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let device = storage.open_into(&db).await.expect("open exact test Store");
    let stored = storage
        .create_exact_opaque_blob("photos", "maintenance", b"maintenance")
        .await;
    db.test_sql(move |database| database.enqueue_blob_delete(&stored, T0))
        .await
        .expect("queue exact maintenance tombstone");
    storage.arm_provider_write_failures();
    let (_temp, store_dir) = temp_store_dir();
    let result = device.run_cycle(&store_dir, None).await;
    let error = result.expect_err("tombstone publication failure fails the cycle");
    assert!(error.contains("drain queued blob tombstones"), "{error}");
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
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
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device")
            .expect("local Store device exists");
        let registration = crate::database::StoreDatabase::new(self)
            .activated_store_device_registration_records()
            .await
            .expect("read activated Store registrations")
            .into_iter()
            .find(|registration| registration.value().device_id.to_string() == local_device)
            .expect("local Store registration is active");
        registration
            .value()
            .store_announcement_activation(registration.reference())
            .expect("derive local Store announcement activation")
            .author_stream_id()
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
        let (root_table, root_id) = (root_table.to_string(), root_id.to_string());
        self.test_sql(move |database| database.make_remote_intent_exists(&root_table, &root_id))
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
    async fn store_package_exists(&self, db: &Database, stream_id: &str, sequence: u64) -> bool;
    async fn local_store_package_exists(&self, db: &Database, sequence: u64) -> bool;
    async fn stored_blob_exists(&self, db: &Database, table: &str, row_id: &str) -> bool;
    async fn retain_store_packages_for_assertion(&self, db: &Database, marker: &[u8]);
    async fn assert_latest_ack_timestamp_is_rfc3339(&self, db: &Database);
}

impl CycleTestStoreOps for TestStore {
    async fn store_package_exists(&self, db: &Database, stream_id: &str, sequence: u64) -> bool {
        let device = self
            .bind_founder_device(db)
            .await
            .expect("bind Store package test device");
        let Some((reference, _commit)) = device
            .load_exact_materialized_commit(stream_id, sequence)
            .await
            .expect("load exact materialized Store commit")
        else {
            return false;
        };
        match device.load_store_package_for_test(&reference).await {
            Ok(package) => package.is_some(),
            Err(crate::sync::store::StoreError::Object(
                coven_protocol::objects::StoreObjectError::Storage(
                    coven_protocol::objects::StorageError::NotFound(_),
                ),
            )) => false,
            Err(error) => panic!("load Store package: {error}"),
        }
    }

    async fn local_store_package_exists(&self, db: &Database, sequence: u64) -> bool {
        let stream_id = db.local_store_stream_id().await;
        self.store_package_exists(db, &stream_id, sequence).await
    }

    async fn stored_blob_exists(&self, db: &Database, table: &str, row_id: &str) -> bool {
        let Some(stored) = db.stored_blob_for_row(table, row_id).await else {
            return false;
        };
        self.contains_stored_blob_object(&stored)
            .await
            .expect("verify exact stored blob object")
    }

    async fn retain_store_packages_for_assertion(&self, db: &Database, marker: &[u8]) {
        let device = self
            .open_into(db)
            .await
            .expect("open exact Store before seeding snapshot");
        let mut writer = device
            .authorize_writer()
            .await
            .expect("authorize snapshot fixture writer");
        writer
            .push_store_snapshot(
                crate::database::CreatedSnapshot {
                    db_image: marker.to_vec(),
                    blobs: Vec::new(),
                },
                coven_protocol::store_commit::CommitFrontier(BTreeMap::new()),
                db.schema_version(),
                T0.to_string(),
            )
            .await
            .expect("publish exact Store snapshot fixture");
    }

    /// The acknowledgement the cycle writes records its completion time as an RFC
    /// 3339 wall-clock string, never the HLC string used to order row writes.
    async fn assert_latest_ack_timestamp_is_rfc3339(&self, db: &Database) {
        let published = store_database(db)
            .latest_local_store_ack()
            .await
            .expect("read latest exact Store acknowledgement")
            .expect("the cycle published an acknowledgement");
        let device = self
            .bind_founder_device(db)
            .await
            .expect("bind acknowledgement inspection Store");
        let local_device = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device")
            .expect("local Store device exists");
        let registration = crate::database::StoreDatabase::new(db)
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

fn fail_exact_create_on(storage: &TestStore, call: usize) {
    storage.fail_exact_create_before_call(call);
}

fn exercise_pre_attempt_abandonment<'a>(
    owner_db: &'a crate::database::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let owner_device = storage
            .bind_store_device(owner_db, owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
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
        assert!(matches!(
            pending
                .status(abandonment.abandonment.attempt_id)
            .expect("load joiner join status"),
            Some(DeviceJoinStatus::Abandoned { abandonment: durable }) if durable == abandonment
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
    owner_db: &'a crate::database::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    interruption: ExactCreateInterruption,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let owner_device = storage
            .bind_store_device(owner_db, owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin exact device join");
        let attempt_id = offer.attempt_id;
        let pending_join = storage
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
        let first = owner_device
            .authorize_device_provider_access(request.clone(), None)
            .await;
        let approval = match interruption {
            ExactCreateInterruption::BeforeVisibility => {
                assert!(
                    first.is_err(),
                    "the injected create fails before visibility"
                );
                assert!(matches!(
                    owner_db
                        .device_join_status(attempt_id, DeviceJoinRole::ProviderAdministrator,)
                        .await
                        .expect("load provider create status"),
                    Some(DeviceJoinStatus::ProviderAccessGrantCreatePending { .. })
                ));
                owner_device
                    .authorize_device_provider_access(request, None)
                    .await
                    .expect("resume provider access grant creation")
            }
            ExactCreateInterruption::AfterVisibility => {
                first.expect("lost create response settles through exact readback")
            }
        };
        let retry = owner_device
            .authorize_device_provider_access((*approval.request).clone(), None)
            .await
            .expect("retry completed provider access authorization");
        assert_eq!(retry, approval);
        assert!(matches!(
            owner_db
                .device_join_status(attempt_id, DeviceJoinRole::ProviderAdministrator)
                .await
                .expect("load completed provider access status"),
            Some(DeviceJoinStatus::AwaitingRegistrationRequest { .. })
        ));
    })
}

fn exercise_post_attempt_cancellation<'a>(
    owner_db: &'a crate::database::StoreDatabase,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    joiner_disposition: JoinerCancellationDisposition,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use crate::sync::store::{
            DeviceJoinRole, DeviceJoinStatus, JoinerJoinTerminal, ProviderAdminJoinTerminal,
        };

        let owner_device = storage
            .bind_store_device(owner_db, owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin exact device join");
        let mut pending_join = storage
            .open_pending_device_join(&pending, member, offer.clone())
            .await
            .expect("bind pending Store join");
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare exact provider access request");
        let approval = owner_device
            .authorize_device_provider_access(access_request, None)
            .await
            .expect("authorize exact provider access");
        let joiner_access_locator = approval.access_grant.grant.locator.clone();
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .expect("prepare exact registration request");
        let mut joiner_closure = storage
            .pending_device_join_observation(&pending, &offer)
            .await
            .expect("open joining device closure")
            .authorize_closure(member);
        let provisional = owner_device
            .accept_device_registration_request(registration_request)
            .await
            .expect("activate exact join attempt");
        let attempt_id = provisional.publication_authorization.attempt.attempt_id;
        let cancellation = owner_device
            .cancel_device_join(provisional.publication_authorization.attempt.clone())
            .await
            .expect("cancel exact active join attempt");
        let cancellation_retry = owner_device
            .cancel_device_join(provisional.publication_authorization.attempt.clone())
            .await
            .expect("retry exact active join cancellation");
        assert_eq!(cancellation_retry, cancellation);

        let administrator_terminal = owner_device
            .close_device_provider_admission(cancellation.clone())
            .await
            .expect("close exact provider admission");
        let administrator_retry = owner_device
            .close_device_provider_admission(cancellation.clone())
            .await
            .expect("retry exact provider admission closure");
        assert_eq!(administrator_retry, administrator_terminal);
        assert!(matches!(
            &administrator_terminal,
            ProviderAdminJoinTerminal::Cancelled(_)
        ));

        let joiner_revocation = ConfirmedWriteRevocation::direct(joiner_access_locator.clone());
        let joiner_terminal = match joiner_disposition {
            JoinerCancellationDisposition::Closure => joiner_closure
                .close(cancellation.clone())
                .await
                .expect("close exact joining device"),
            JoinerCancellationDisposition::WriteRevocation => owner_device
                .revoke_joining_device_writes(cancellation.clone(), &joiner_revocation)
                .await
                .expect("revoke absent joining-device writes"),
        };
        if matches!(
            joiner_disposition,
            JoinerCancellationDisposition::WriteRevocation
        ) {
            let coven_protocol::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack,
            } = &provisional.request.expected_registration.acknowledgements
            else {
                panic!("joining registration has a non-Store acknowledgement stream");
            };
            let mut expected_slots = vec![
                provisional.request.registration_slot.clone(),
                first_ack.clone(),
            ];
            if let coven_protocol::store_commit::device_join_exchange::DeviceProviderResponseReservation::CrossPrincipal {
                response_slot,
            } = &provisional.request.response
            {
                expected_slots.push(response_slot.clone());
            }
            assert_eq!(
                joiner_revocation.requests(),
                vec![WriteRevocationRequest {
                    producer: coven_protocol::store_commit::device_join_exchange::DeviceJoinProducer::Joiner,
                    authority: coven_protocol::store_commit::device_join_exchange::ProviderWriteAuthorityRef::MemberAccess(
                        provisional.request.approval.access_grant.grant_ref.clone(),
                    ),
                    locator: joiner_access_locator.clone(),
                    protected_slots: expected_slots,
                }],
            );
        }
        let joiner_retry = match joiner_disposition {
            JoinerCancellationDisposition::Closure => joiner_closure
                .close(cancellation.clone())
                .await
                .expect("retry exact joining-device closure"),
            JoinerCancellationDisposition::WriteRevocation => {
                let revocation = ConfirmedWriteRevocation::direct(joiner_access_locator);
                let terminal = owner_device
                    .revoke_joining_device_writes(cancellation.clone(), &revocation)
                    .await
                    .expect("retry absent joining-device write revocation");
                assert!(revocation.requests().is_empty());
                terminal
            }
        };
        assert_eq!(joiner_retry, joiner_terminal);
        assert!(match joiner_disposition {
            JoinerCancellationDisposition::Closure =>
                matches!(&joiner_terminal, JoinerJoinTerminal::Cancelled(_)),
            JoinerCancellationDisposition::WriteRevocation =>
                matches!(&joiner_terminal, JoinerJoinTerminal::WriteRevoked(_)),
        });
        assert!(owner_db
            .device_join_actions()
            .await
            .expect("enumerate terminal Store join actions")
            .contains(
                &crate::sync::store::DeviceJoinAction::TransferProviderAdminTerminal(
                    administrator_terminal.clone(),
                ),
            ));
        let joiner_action =
            crate::sync::store::DeviceJoinAction::TransferJoinerTerminal(joiner_terminal.clone());
        match joiner_disposition {
            JoinerCancellationDisposition::Closure => assert_eq!(
                pending
                    .actions()
                    .expect("enumerate terminal joiner actions"),
                vec![joiner_action],
            ),
            JoinerCancellationDisposition::WriteRevocation => assert!(owner_db
                .device_join_actions()
                .await
                .expect("enumerate replacement joiner terminal")
                .contains(&joiner_action),),
        }

        storage.fail_exact_create_before_call(1);
        let interrupted_cleanup = owner_device
            .prepare_device_join_cleanup(
                cancellation.clone(),
                administrator_terminal.clone(),
                joiner_terminal.clone(),
            )
            .await;
        assert!(
            interrupted_cleanup.is_err(),
            "the cleanup-receipt create interruption surfaces"
        );
        assert!(matches!(
            owner_db
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load interrupted cleanup status"),
            Some(DeviceJoinStatus::CleanupReceiptCreatePending { .. })
        ));
        let receipt = owner_device
            .prepare_device_join_cleanup(
                cancellation.clone(),
                administrator_terminal.clone(),
                joiner_terminal.clone(),
            )
            .await
            .expect("resume exact cleanup receipt");
        let receipt_retry = owner_device
            .prepare_device_join_cleanup(cancellation, administrator_terminal, joiner_terminal)
            .await
            .expect("retry exact cleanup receipt");
        assert_eq!(receipt_retry, receipt);

        let activation = owner_device
            .activate_device_join_cleanup(receipt.clone())
            .await
            .expect("activate exact cleanup receipt");
        let activation_retry = owner_device
            .activate_device_join_cleanup(receipt)
            .await
            .expect("retry exact cleanup activation");
        assert_eq!(activation_retry, activation);

        let owner_complete = owner_device
            .complete_owner_device_join_cleanup(activation.clone())
            .await
            .expect("complete exact owner cleanup");
        let owner_complete_retry = owner_device
            .complete_owner_device_join_cleanup(activation.clone())
            .await
            .expect("retry exact owner cleanup completion");
        assert_eq!(owner_complete_retry, owner_complete);
        let mut forged_activation = activation.clone();
        forged_activation.activation.commit_hash =
            coven_protocol::store_commit::ObjectHash::digest(b"forged cleanup activation");
        let mut cleanup_observation = storage
            .pending_device_join_observation(&pending, &offer)
            .await
            .expect("open joiner cleanup observation");
        assert!(
            cleanup_observation
                .accept_cleanup(forged_activation)
                .await
                .is_err(),
            "joiner cleanup must reject an activation whose exact Store commit was not verified",
        );
        cleanup_observation
            .accept_cleanup(activation.clone())
            .await
            .expect("accept exact joiner cleanup activation");
        let joiner_complete = pending
            .complete_joiner_cleanup(activation.clone())
            .expect("complete exact joiner cleanup");
        let joiner_complete_retry = pending
            .complete_joiner_cleanup(activation)
            .expect("retry exact joiner cleanup completion");
        assert_eq!(joiner_complete_retry, joiner_complete);
        assert!(matches!(
            owner_db
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load owner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
        assert!(matches!(
            pending
                .status(attempt_id)
                .expect("load joiner cancellation status"),
            Some(DeviceJoinStatus::CleanupActivated { .. })
        ));
    })
}

#[tokio::test]
async fn missing_provider_administrator_writes_are_revoked_and_cleaned_up() {
    use coven_protocol::membership::MemberRole;
    use coven_protocol::objects::{
        ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, StoreProviderBinding,
    };

    let owner = UserKeypair::generate();
    let database = open_test_db();
    let home = crate::sync::test_helpers::test_cloud_home_with_binding(ResolvedProviderBinding {
        store: StoreProviderBinding::Dropbox {
            namespace_id: "revocation-namespace".to_string(),
        },
        device: ProviderDeviceBinding {
            principal: ProviderPrincipalId::Dropbox {
                account_id: "administrator-account".to_string(),
            },
        },
    });
    let storage = TestStore::create(
        &database,
        "cross-principal-revocation-store",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create cross-principal test Store");
    let member = UserKeypair::generate();
    storage
        .invite_member(
            &database,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([48; 32]),
            "Test Store",
        )
        .await
        .expect("invite exact Member identity");
    let owner_db = crate::database::StoreDatabase::new(&database);
    let owner_db = &owner_db;
    let storage = &storage;
    let home = home.as_ref();
    let owner = &owner;
    let member = &member;

    Box::pin(async move {
        use coven_protocol::objects::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
        };
        use crate::sync::store::{JoinerJoinTerminal, ProviderAdminJoinTerminal};

        let owner_device = storage
            .bind_store_device(owner_db, owner)
            .await
            .expect("bind owner Store");
        let owner_binding = crate::storage::SyncStorage::provider_binding(&*storage.storage())
            .await
            .expect("resolve owner provider binding");
        let coven_protocol::objects::StoreProviderBinding::Dropbox { namespace_id } =
            &owner_binding.store
        else {
            panic!("cross-principal test Store is not Dropbox");
        };
        let peer_home =
            std::sync::Arc::new(home.clone().with_provider_binding(ResolvedProviderBinding {
                store: owner_binding.store.clone(),
                device: ProviderDeviceBinding {
                    principal: ProviderPrincipalId::Dropbox {
                        account_id: "member-account".to_string(),
                    },
                },
            }));
        let peer_storage: std::sync::Arc<dyn crate::storage::SyncStorage> = std::sync::Arc::new(
            crate::storage::CloudSyncStorage::new(
                peer_home.clone(),
                crate::storage::CloudCipher::Encrypted(
                    coven_keys::encryption::EncryptionService::from_key([42; 32]),
                ),
                crate::storage::BlobPathScheme::Hashed,
                "cross-principal-revocation-store",
                member.clone(),
            )
            .expect("create peer exact storage"),
        );
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(member))
            .await
            .expect("begin cross-principal device join");
        let provider_locator = offer.provider_admin.access.clone();
        let join_history =
            crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                .open_pinned(peer_storage.as_ref(), &offer.store_root)
                .await
                .expect("open pending cross-principal Store history");
        let observation = crate::sync::store::PendingDeviceJoinObservation::new(
            &pending,
            &peer_storage,
            join_history,
            offer.attempt_id,
        );
        let mut pending_join = crate::sync::store::PendingDeviceJoinAuthority::open(
            observation,
            member,
            offer.clone(),
        )
        .await
        .expect("bind pending cross-principal Store join");
        let request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare cross-principal access request");
        let access_administrator = crate::sync::test_helpers::TestDropboxAccessAdministrator {
            namespace_id: namespace_id.clone(),
        };
        let approval = owner_device
            .authorize_device_provider_access(request, Some(&access_administrator))
            .await
            .expect("authorize cross-principal provider access");
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .expect("prepare cross-principal registration request");
        let join_history =
            crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                .open_pinned(peer_storage.as_ref(), &offer.store_root)
                .await
                .expect("open cross-principal closure history");
        let mut joiner_closure = crate::sync::store::PendingDeviceJoinObservation::new(
            &pending,
            &peer_storage,
            join_history,
            offer.attempt_id,
        )
        .authorize_closure(member);
        let provisional = owner_device
            .accept_device_registration_request(registration_request)
            .await
            .expect("activate cross-principal join attempt");
        let cancellation = owner_device
            .cancel_device_join(provisional.publication_authorization.attempt.clone())
            .await
            .expect("cancel cross-principal join attempt");
        owner_db
            .forget_provider_administrator_journals_for_test()
            .await
            .expect("remove unavailable provider administrator's local journal");
        let revocation = ConfirmedWriteRevocation::direct(provider_locator.clone());
        let administrator_terminal = owner_device
            .revoke_device_provider_admission_writes(cancellation.clone(), &revocation)
            .await
            .expect("revoke absent provider-administrator writes");
        let coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
            &provisional.request.approval.admission
        else {
            panic!("missing-provider test did not create a cross-principal challenge");
        };
        assert_eq!(
            revocation.requests(),
            vec![WriteRevocationRequest {
                producer: coven_protocol::store_commit::device_join_exchange::DeviceJoinProducer::ProviderAdministrator,
                authority: coven_protocol::store_commit::device_join_exchange::ProviderWriteAuthorityRef::ProviderAdministrator(
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
        let administrator_retry = owner_device
            .revoke_device_provider_admission_writes(cancellation.clone(), &retry_revocation)
            .await
            .expect("retry provider-administrator write revocation");
        assert!(retry_revocation.requests().is_empty());
        assert_eq!(administrator_retry, administrator_terminal);
        assert!(matches!(
            &administrator_terminal,
            ProviderAdminJoinTerminal::WriteRevoked(_)
        ));
        let joiner_terminal = joiner_closure
            .close(cancellation.clone())
            .await
            .expect("close cross-principal joining device");
        assert!(matches!(&joiner_terminal, JoinerJoinTerminal::Cancelled(_)));
        let receipt = owner_device
            .prepare_device_join_cleanup(cancellation, administrator_terminal, joiner_terminal)
            .await
            .expect("prepare cleanup with revoked provider administrator");
        let activation = owner_device
            .activate_device_join_cleanup(receipt)
            .await
            .expect("activate cleanup with revoked provider administrator");
        owner_device
            .complete_owner_device_join_cleanup(activation.clone())
            .await
            .expect("complete owner cleanup");
        let join_history =
            crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                .open_pinned(peer_storage.as_ref(), &offer.store_root)
                .await
                .expect("open cross-principal cleanup history");
        let mut cleanup_observation = crate::sync::store::PendingDeviceJoinObservation::new(
            &pending,
            &peer_storage,
            join_history,
            offer.attempt_id,
        );
        cleanup_observation
            .accept_cleanup(activation.clone())
            .await
            .expect("accept exact joiner cleanup activation");
        pending
            .complete_joiner_cleanup(activation)
            .expect("complete joiner cleanup");
    })
    .await;
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
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                let blob_decl =
                    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
                let db = open_test_db_with_blob(blob_decl.clone());
                let storage = Arc::new(
                    cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home())
                        .await,
                );
                let (_tmp, ld) = temp_store_dir();
                storage
                    .retain_store_packages_for_assertion(&db, b"existing-pending-upload-snapshot")
                    .await;
                let peer = UserKeypair::generate();
                storage
                    .invite_member(
                        &db,
                        &keypair,
                        &pubkey_hex(&peer),
                        None,
                        coven_protocol::membership::MemberRole::Member,
                        &EncryptionService::from_key([42; 32]),
                        "Test Store",
                    )
                    .await
                    .expect("invite exact pending-upload peer");
                let db_b = open_test_db_with_blob(blob_decl);
                let peer_device = storage
                    .activate_joined_device(&db, &db_b, &peer, T0)
                    .await
                    .expect("activate exact joined test device");
                let device = storage
                    .open_into(&db)
                    .await
                    .expect("bind exact pending-upload device");
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                    ld.clone(),
                )
                .await
                .expect("settle exact pending-upload peer activation");

                // A slow/stuck upload for some OTHER unit is pending the whole time.
                db.seed_stuck_blob_upload_for_test(T0)
                    .await
                    .expect("seed exact pending upload");

                // One shareable note (its blobs are up → gate on) and one still-private note
                // (its blobs aren't up yet → gate off; the host hasn't flipped it).
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
                )
                .await;
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('priv', 'NotYet', NULL, 0, '0000000002000-0000-M', '2026-01-01')",
                )
                .await;

                // The changeset pushes despite the pending upload — no global deferral.
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device,
                    ld.clone(),
                )
                .await
                .expect("publish gated-true write beside pending upload");

                // The activated peer pulls: it gets the shareable row, never the gated-false one.
                peer_device
                    .pull_store(&ld)
                    .await
                    .expect("pull exact pending-upload peer");
                assert_eq!(
                    db_b.query_test_text("SELECT title FROM notes WHERE id = 'pub'")
                        .await,
                    "Shareable",
                    "the shareable note reaches the peer",
                );
                assert!(
        !db_b
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'priv'")
            .await,
        "a gated-false row is still withheld — that is what holds a not-yet-uploaded unit",
    );
            })
            .await
            .expect("pending-upload gate orchestration");
        })
        .await;
}

/// A gated-false row is withheld until its gate flips on, then it propagates: the
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (coven
/// flips the gate when a manage's blobs land; here the flip is written directly.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                let blob_decl =
                    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
                let db = open_test_db_with_blob(blob_decl.clone());
                let storage = Arc::new(
                    cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home())
                        .await,
                );
                let (_tmp, ld) = temp_store_dir();
                let peer = UserKeypair::generate();
                storage
                    .invite_member(
                        &db,
                        &keypair,
                        &pubkey_hex(&peer),
                        None,
                        coven_protocol::membership::MemberRole::Member,
                        &EncryptionService::from_key([42; 32]),
                        "Test Store",
                    )
                    .await
                    .expect("invite exact gate-flip peer");
                let db_b = open_test_db_with_blob(blob_decl);
                let peer_device = storage
                    .activate_joined_device(&db, &db_b, &peer, T0)
                    .await
                    .expect("activate exact joined test device");
                let device = storage
                    .open_into(&db)
                    .await
                    .expect("bind exact gate-flip device");
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                    ld.clone(),
                )
                .await
                .expect("settle exact gate-flip peer activation");

                // A note whose blobs aren't up yet: gate off.
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
                )
                .await;
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                    ld.clone(),
                )
                .await
                .expect("publish gated-false Store write");

                peer_device
                    .pull_store(&ld)
                    .await
                    .expect("pull gated-false Store state");
                assert!(
                    !db_b
                        .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
                        .await,
                    "a gated-false row must not reach a peer",
                );

                // The blobs land; the host flips the gate on. The next cycle re-emits the
                // now-shareable row.
                db.execute_test_host_write(
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device,
                    ld.clone(),
                )
                .await
                .expect("publish gate-flip Store write");

                // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
                // re-emits it at seq 1. Re-pull from empty positions to pick it up wherever it
                // landed.
                peer_device
                    .pull_store(&ld)
                    .await
                    .expect("pull gate-flip Store state");
                assert_eq!(
                    db_b.query_test_text("SELECT title FROM notes WHERE id = 'n1'")
                        .await,
                    "Album Title",
                    "once its gate flips on, the row reaches the peer",
                );
            })
            .await
            .expect("gate-flip propagation orchestration");
        })
        .await;
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
    let storage =
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    db.seed_stuck_blob_upload_for_test(T0)
        .await
        .expect("seed exact pending upload");

    let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
    cycle_device
        .run_cycle(&ld, None)
        .await
        .expect("run snapshot cycle");
    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

#[tokio::test]
async fn initial_snapshot_uploads_remote_root_host_blobs_before_publish() {
    let keypair = UserKeypair::generate();
    let db = open_test_db_schema(
        crate::sync::test_helpers::test_synced_tables_remote_root_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        )),
        test_migrations(),
    );
    let storage =
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    // Remove the seed writes so the cycle takes the initial-snapshot path; the rows
    // still reach the cloud through the snapshot, which reads them from the db.
    let _ = db.capture_test_changeset(&[]).await;

    let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
    cycle_device
        .run_cycle(&ld, None)
        .await
        .expect("run initial snapshot cycle");

    let stored = db
        .stored_blob_for_row("note_photos", "cover1")
        .await
        .expect("the snapshot activates its exact host blob binding");
    storage
        .storage()
        .verify_blob_object(&stored)
        .await
        .expect("the blob referenced by the initial snapshot exists");
    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "the snapshot metadata publishes after its referenced blob exists",
    );
}

#[tokio::test]
async fn initial_snapshot_does_not_publish_when_host_blob_upload_fails() {
    let keypair = UserKeypair::generate();
    let tables = crate::sync::test_helpers::test_synced_tables_remote_root_with_blob(
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
    );
    let db = open_test_db_schema(tables.clone(), test_migrations());
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (_tmp, ld) = temp_store_dir();
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    assert_eq!(db.pending_write_count().await, 0);
    let device = storage.open_into(&db).await.expect("open exact test Store");
    let restore_membership = device
        .restore_membership()
        .await
        .expect("load exact Store restore membership");

    let cycle_storage = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let failed = run_cycle_in_task(Arc::clone(&cycle_storage), device.clone(), ld.clone())
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
    let installed_bindings = db
        .test_sql(|database| {
            database.table_row_count(crate::database::DatabaseTestTable::named(
                "row_blob_locators",
            ))
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
        storage.storage().verify_blob_object(&rejected[0]).await,
        Err(StorageError::NotFound(_))
    ));
    assert!(
        db.latest_store_snapshot_meta().await.is_none(),
        "snapshot metadata is not published when a referenced blob upload fails",
    );

    let pending_reference = pending.reference.clone();
    let pending_image = pending.meta.value.image.clone();
    let pending_spool = pending.blobs[0]
        .spool_path
        .clone()
        .expect("failed publication retains exact spool");
    db.execute_test_sql(
        "UPDATE note_photos
         SET size = 6,
             hash = 'b7cb0795b8e42b33917c4bc2007f7a3f49c6e2777927b004c1a2ff587fcb1a7f',
             _updated_at = '0000000002000-0000-M'
         WHERE id = 'cover1'",
    )
    .await;
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
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
        coven_protocol::blob::RowBlobAuthority::PendingRemote(
            coven_protocol::blob::locator::RemoteAudience::Store
        )
    ));
    assert!(live.stored().is_none());
    storage
        .storage()
        .verify_blob_object(&rejected[0])
        .await
        .expect("retry publishes exact retained blob");
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("load completed snapshot publication")
        .is_none());
    assert!(!pending_spool.exists());

    ld.remove_local_blob("photos", "cover1")
        .await
        .expect("remove source blob before restore");
    let (restore_temp, restore_dir) = temp_store_dir();
    let restore_path = restore_dir.db_path();
    let restorer_identity = coven_keys::keys::UserKeypair::generate();
    let bootstrap = storage
        .prepare_snapshot_bootstrap(
            &restore_membership.membership_floor,
            db.schema_version(),
            &restore_path,
            &restorer_identity,
        )
        .await
        .expect("verify snapshot-only blob bootstrap");
    let restored = bootstrap
        .install(
            &restore_dir,
            tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "restored-snapshot-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
            None,
        )
        .await
        .expect("install snapshot-only blob bootstrap");
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    assert_eq!(
        restored
            .reconcile_snapshot_blobs(&cancel_rx)
            .await
            .expect("reconcile restored snapshot blob"),
        crate::sync::store::SnapshotBlobReconcile::Complete,
    );
    assert_eq!(
        restored
            .read_local_blob_for_test(&restore_dir, "note_photos", "cover1")
            .await
            .expect("read restored snapshot blob"),
        b"cover".to_vec(),
    );
    drop(restore_temp);
}

#[tokio::test]
async fn initial_snapshot_removes_current_spool_when_blob_preparation_fails() {
    let keypair = UserKeypair::generate();
    let tables = vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .remote_root(),
        SyncedTable::new(
            "note_photos",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        )),
    ];
    let db = open_test_db_schema(tables, test_migrations());
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (_tmp, store_dir) = temp_store_dir();
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &store_dir, "photos", "cover1", b"cover",
    )
    .await
    .expect("store host-provided snapshot blob");
    assert_eq!(db.pending_write_count().await, 0);
    let device = storage.open_into(&db).await.expect("open exact test Store");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_prepare(Arc::clone(
        &storage,
    )));

    let error = run_cycle_in_task(Arc::clone(&interceptor), device, store_dir.clone())
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
        coven_foundation::store_dir::StoreDir::read_local_blob(&store_dir, "photos", "cover1", 5)
            .await
            .expect("read retained snapshot source"),
        Some(b"cover".to_vec()),
    );
}

#[tokio::test]
async fn snapshot_blob_spool_cleanup_survives_database_restart() {
    let keypair = UserKeypair::generate();
    let tables = crate::sync::test_helpers::test_synced_tables_remote_root_with_blob(
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
    );
    let database_dir = tempfile::tempdir().expect("snapshot cleanup database directory");
    let database_path = database_dir.path().join("store.db");
    let open = || {
        Database::open(
            &database_path,
            tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "snapshot-cleanup-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open snapshot cleanup database")
    };
    let db = open();
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (_store_temp, store_dir) = temp_store_dir();
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &store_dir, "photos", "cover1", b"cover",
    )
    .await
    .expect("store cleanup source blob");
    let device = storage.open_into(&db).await.expect("open cleanup Store");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(interceptor, device, store_dir.clone())
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
    let store = crate::sync::store::Store::load(
        crate::database::StoreDatabase::new(&reopened),
        storage.storage(),
        store_dir.clone(),
        keypair,
    )
    .await
    .expect("load snapshot cleanup Store after restart");
    assert!(store
        .authorize_writer()
        .await
        .expect("authorize snapshot cleanup writer after restart")
        .resume_snapshot_publication()
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
    let tables = vec![SyncedTable::new(
        "assets",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .remote_root()
    .carries_blob(
        BlobDecl::new("assets", Provenance::HostProvided, CacheFill::CacheEager)
            .with_id_column("blob_id"),
    )];
    let migrations = vec![crate::Migration::sql(
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
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (_temp, store_dir) = temp_store_dir();
    let hash = coven_protocol::blob::content_hash(b"shared");
    db.execute_test_sql(&format!(
        "INSERT INTO assets (id, blob_id, size, hash, _updated_at) VALUES
             ('row-a', 'blob-shared', 6, '{hash}', '0000000001000-0000-M'),
             ('row-b', 'blob-shared', 6, '{hash}', '0000000001000-0000-M')"
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &store_dir,
        "assets",
        "blob-shared",
        b"shared",
    )
    .await
    .expect("store shared snapshot blob");
    let device = storage
        .open_into(&db)
        .await
        .expect("open shared blob Store");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create_on(
        Arc::clone(&storage),
        2,
    ));
    run_cycle_in_task(Arc::clone(&interceptor), device, store_dir)
        .await
        .expect("publish coalesced shared snapshot blob");
    assert_eq!(interceptor.rejected_blobs().len(), 1);
    let (bindings, objects) = db
        .test_sql(|database| {
            Ok((
                database.table_row_count(crate::database::DatabaseTestTable::named(
                    "row_blob_locators",
                ))?,
                database
                    .table_row_count(crate::database::DatabaseTestTable::named("blob_locators"))?,
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
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (external_dir, store_dir) = temp_store_dir();
    let external_path = external_dir.path().join("audio1.flac");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&external_path, b"AUDIO")
        .await
        .expect("write external snapshot blob");
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('n1', 'Remote', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
        "INSERT INTO note_photos
             (id, note_id, kind, size, hash, _updated_at, created_at)
             VALUES ('audio1', 'n1', 'audio', 5, '{}',
                     '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"AUDIO"),
    ))
    .await;
    crate::database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "audio1", &external_path)
        .await;
    db.execute_test_sql(
        "UPDATE notes SET shared = 1, _updated_at = '0000000002000-0000-M' WHERE id = 'n1'",
    )
    .await;
    let device = storage.open_into(&db).await.expect("open user blob Store");
    let device_id = device.device_id.clone();
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let error = run_cycle_in_task(Arc::clone(&interceptor), device.clone(), store_dir.clone())
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

    let stored = storage
        .create_exact_opaque_blob("audio", "audio1", b"AUDIO")
        .await;
    assert!(!store_dir
        .outbound_blob_spool_path(stored.locator().locator_hash())
        .exists());
    let registration = crate::database::StoreDatabase::new(&db)
        .activated_store_device_registration_records()
        .await
        .expect("load exact Store registrations")
        .into_iter()
        .find(|registration| registration.value().device_id.to_string() == device_id)
        .expect("local Store registration is activated");
    let record = coven_protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
        &stored,
        coven_protocol::remote_object::SnapshotObjectOwner {
            activation: registration
                .value()
                .store_snapshot_activation(registration.reference())
                .expect("derive exact Store snapshot activation")
                .activation_id(),
            generation: 0,
        },
    )
    .expect("activate exact user blob for the initial snapshot");
    let object_id = record.object_id().to_string();
    let state = serde_json::to_string(&record).expect("serialize exact user blob state");
    let locator_hash = stored.locator().locator_hash().to_string();
    let audience = serde_json::to_string(&coven_protocol::audience_package::PackageAudience::Store)
        .expect("serialize Store audience");
    db.test_sql(move |database| {
        database.install_blob_binding(
            &object_id,
            &state,
            &locator_hash,
            "note_photos",
            "audio1",
            "id",
            "0000000001000-0000-M",
            &audience,
        )
    })
    .await
    .expect("install exact activated user blob binding");
    tokio::fs::remove_file(&external_path)
        .await
        .expect("remove external source before exact-binding retry");

    run_cycle_in_task(Arc::clone(&interceptor), device, store_dir)
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
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(
        &db,
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in protocol_state",
    );
    let membership = storage
        .bind_device(&db, &owner)
        .await
        .expect("load founder Store")
        .membership_for_test()
        .await
        .expect("load exact founder membership");
    assert!(
        membership.is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    storage
        .open_into(&db)
        .await
        .expect("re-open Store through the pinned founder");
    let owner_before = db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let graph = store_database(&db)
        .local_store_founder_graph()
        .await
        .expect("read founder graph")
        .expect("founder graph exists");
    let crate::database::DurableFounderMembership { head, .. } = graph.membership;
    storage
        .storage()
        .delete_protocol_object(&head.object)
        .await
        .expect("delete exact founder head");
    assert!(
        storage.open_into(&db).await.is_err(),
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
    let storage = TestStore::create(
        &creator_db,
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");
    let opened_db = open_test_db();
    assert_eq!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read founder device genesis before anchoring"),
        None,
    );

    storage
        .open_into(&opened_db)
        .await
        .expect("open Store through its founder");

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
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db = open_test_db();
    let storage = TestStore::create(
        &db,
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");
    db.delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove local owner pin");
    storage
        .open_into(&db)
        .await
        .expect("re-open Store through its founder");
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the exact founder restores its owner pin",
    );

    let attacker = UserKeypair::generate();
    let attacker_db = open_test_db();
    let seeded = TestStore::create(
        &attacker_db,
        "foreign-store",
        attacker,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create foreign exact Store");
    let fresh_db = open_test_db();
    let (_foreign_store_dir_temp, foreign_store_dir) = temp_store_dir();
    assert!(
        crate::sync::store::Store::open(
            store_database(&fresh_db),
            seeded.storage(),
            foreign_store_dir,
            &seeded.root,
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
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

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
    let (_store_temp, store_dir) = temp_store_dir();

    cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&db),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&db),
            store_dir,
        ),
        storage,
        owner,
        cycle::StoreInitialization::CreateStore,
        None,
    )
    .await
    .expect("prepare plaintext storage")
    .initialize()
    .await
    .expect("initialize plaintext storage");

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let cursor_count = db
        .test_sql(|database| database.protocol_state_prefix_count("membership_head_cursor/"))
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn initialization_refuses_a_founder_entry_without_its_store_protocol_root() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let seeded_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    ));
    let seed_db = open_test_db();
    let seeded_device = crate::sync::test_helpers::TestDevice::create(
        &seed_db,
        seeded_storage.clone(),
        "test-lib",
        owner.clone(),
    )
    .await
    .expect("create exact Store fixture");
    let root = seeded_device.store_root().clone();
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
        owner.clone(),
    );
    let (_store_temp, store_dir) = temp_store_dir();

    let prepared = cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&db),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&db),
            store_dir,
        ),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    .expect("prepare Store opening");
    let error = match prepared.initialize().await {
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
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .local_store_root_ref()
            .await
            .unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_refuses_a_foreign_founder_without_store_protocol_root() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    ));
    let attacker_db = open_test_db();
    let attacker_device = crate::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_storage.clone(),
        "test-lib",
        attacker.clone(),
    )
    .await
    .expect("create foreign exact Store fixture");
    let root = attacker_device.store_root().clone();
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
        owner.clone(),
    );
    let db = open_test_db();
    let (_store_temp, store_dir) = temp_store_dir();
    let prepared = cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&db),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&db),
            store_dir,
        ),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    .expect("prepare foreign Store opening");
    let error = match prepared.initialize().await {
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
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let seeded_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    ));
    let seed_db = open_test_db();
    let seeded_device = crate::sync::test_helpers::TestDevice::create(
        &seed_db,
        seeded_storage.clone(),
        "test-lib",
        owner.clone(),
    )
    .await
    .expect("create committed exact Store fixture");
    let root = seeded_device.store_root().clone();
    let cloud_before = cloud_objects(&home);

    let db = open_test_db();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let (_store_temp, store_dir) = temp_store_dir();
    cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&db),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&db),
            store_dir,
        ),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await
    .expect("prepare committed founder opening")
    .initialize()
    .await
    .expect("accept the identity's committed founder");

    assert_eq!(cloud_objects(&home), cloud_before);
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let cursor_count = db
        .test_sql(|database| database.protocol_state_prefix_count("membership_head_cursor/"))
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn plaintext_initialization_refuses_a_committed_foreign_founder_without_mutation() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    ));
    let attacker_db = open_test_db();
    let attacker_device = crate::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_storage.clone(),
        "test-lib",
        attacker.clone(),
    )
    .await
    .expect("create committed foreign Store");
    let root = attacker_device.store_root().clone();
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
    let (_store_temp, store_dir) = temp_store_dir();

    assert!(
        cycle::PreparedSyncComponents::prepare(
            crate::database::StoreDatabase::new(&db),
            store_dir.clone(),
            crate::sync::test_owner_graph::local_blob_access(
                crate::database::StoreDatabase::new(&db),
                store_dir,
            ),
            victim_storage,
            victim,
            cycle::StoreInitialization::OpenStore {
                expected_store_root: root,
            },
            None,
        )
        .await
        .expect("prepare foreign founder opening")
        .initialize()
        .await
        .is_err(),
        "a committed foreign founder prevents initialization",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .local_store_root_ref()
            .await
            .unwrap(),
        None,
    );
    let watermark_count = db
        .test_sql(|database| database.protocol_state_prefix_count("membership_head_seq/"))
        .await
        .unwrap();
    assert_eq!(watermark_count, 0);
    let cloud_after = cloud_objects(&home);
    assert_eq!(cloud_after, cloud_before, "cloud objects are unchanged");
}

#[tokio::test]
async fn initialization_rejects_an_identity_other_than_the_storage_identity() {
    let owner = UserKeypair::generate();
    let other = UserKeypair::generate();
    let db = open_test_db();
    let storage = cycle_cloud_storage(
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    let (_store_temp, store_dir) = temp_store_dir();

    let result = cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&db),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&db),
            store_dir,
        ),
        storage,
        other,
        cycle::StoreInitialization::CreateStore,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(cycle::InitSyncError::StorageIdentityMismatch)
    ));
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
        let storage = Arc::new(cycle_cloud_storage(
            Arc::new(home.clone()),
            cipher.clone(),
            blob_paths,
            "test-lib",
            owner.clone(),
        ));
        let (_store_temp, store_dir) = temp_store_dir();
        db.set_protocol_state(
            coven_protocol::objects::ROTATION_GATE_STATE_KEY,
            "invalid rotation gate",
        )
        .await
        .unwrap();
        assert!(
            cycle::PreparedSyncComponents::prepare(
                crate::database::StoreDatabase::new(&db),
                store_dir.clone(),
                crate::sync::test_owner_graph::local_blob_access(
                    crate::database::StoreDatabase::new(&db),
                    store_dir,
                ),
                storage.clone(),
                owner,
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
            storage.pending_rotation_generation_for_test(),
            None,
            "the in-memory pending-rotation marker is not restored",
        );
        assert_eq!(
            db.get_protocol_state(coven_protocol::objects::ROTATION_GATE_STATE_KEY)
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

use coven_protocol::objects::StorageError;

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
        reject_create_call: Option<usize>,
        reject_prepare_call: Option<usize>,
        allocate_calls: AtomicUsize,
        prepare_calls: AtomicUsize,
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
            inner,
            interceptor: Arc::new(interceptor),
        }
    }

    fn rejected_blobs(&self) -> Vec<coven_protocol::blob::locator::StoredBlobRef> {
        self.interceptor.rejected_blobs()
    }

    fn blob_write_calls(&self) -> (usize, usize, usize) {
        self.interceptor.blob_write_calls()
    }

    async fn activate_joined_device(
        &self,
        observer_db: &Database,
        joining_db: &Database,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<crate::sync::test_helpers::TestDevice, String> {
        self.inner
            .activate_joined_device(observer_db, joining_db, joining_identity, published_at)
            .await
    }

    async fn run_sync_cycle(
        &self,
        device: &TestDevice,
        store_dir: &StoreDir,
    ) -> Result<cycle::SyncCycleResult, cycle::SyncCycleFailure> {
        device
            .run_cycle_with_interceptor(
                &SystemClock,
                None,
                store_dir,
                None,
                self.interceptor.clone(),
            )
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
                let (_tmp, ld) = temp_store_dir();
                // A peer A has published one changeset (an insert of note 'a1') to shared
                // storage, so M's cycle has something to fetch — the await we inject at.
                let producer_db = open_test_db();
                let inner = cycle_test_store(
                    &producer_db,
                    &keypair,
                    crate::sync::test_helpers::test_cloud_home(),
                )
                .await;
                let producer_device = inner
                    .founder_device()
                    .await
                    .expect("retain producer Store device");
                let a_src = open_test_db();
                let a_cs = a_src
                    .capture_test_changeset(&[
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
                    ])
                    .await;
                // M's database. The injector runs this INSERT into M at the package-read
                // await, mid-pull.
                let db_m = open_test_db();
                let device_m = inner
                    .activate_joined_device(&producer_db, &db_m, &keypair, T0)
                    .await
                    .expect("activate exact joined test device");
                inner
                    .retain_store_packages_for_assertion(&db_m, b"existing-host-write-snapshot")
                    .await;
                let peer_sequence = producer_device
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
                let device_c = storage
                    .activate_joined_device(&producer_db, &db_c, &keypair, T0)
                    .await
                    .expect("activate exact joined test device");

                drop(a_src);
                drop(producer_db);
                tokio::spawn(async move {
                    let cycle = InterceptedCycle::new(&storage, device_m, &ld);
                    // Cycle 1: M pulls A's changeset; the host write fires mid-pull.
                    cycle.run().await;

                    // (a) The injected row is present locally on M.
                    assert!(
                        db_m.test_row_exists("SELECT 1 FROM notes WHERE id = 'm_mid'")
                            .await,
                        "the package read injects the host write into M",
                    );
                    assert_eq!(
                        db_m.query_test_text("SELECT title FROM notes WHERE id = 'm_mid'")
                            .await,
                        "WrittenMidCycle",
                        "the mid-cycle host write committed to M's local db",
                    );

                    // (b) The injected row has its own pending write. Cycle 2 publishes it. A fresh
                    // peer C pulls M's output and must receive 'm_mid'.
                    cycle.run().await;

                    device_c
                        .pull_store(&ld)
                        .await
                        .expect("pull injected host write into C");
                    assert!(
                        db_c.test_row_exists("SELECT 1 FROM notes WHERE id = 'm_mid'")
                            .await,
                        "M's next Store commit carries the injected host write",
                    );
                    assert_eq!(
                        db_c.query_test_text("SELECT title FROM notes WHERE id = 'm_mid'")
                            .await,
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
    let (_tmp, ld) = temp_store_dir();
    // Peer A publishes a changeset; M pulls and applies it in cycle 1.
    let producer_db = open_test_db();
    let storage = Arc::new(
        cycle_test_store(
            &producer_db,
            &keypair,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await,
    );
    let producer_device = storage
        .founder_device()
        .await
        .expect("retain producer Store device");
    let db_m = open_test_db();
    let device_m = storage
        .activate_joined_device(&producer_db, &db_m, &keypair, T0)
        .await
        .expect("activate exact joined test device");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let a_src = open_test_db();
    let a_cs = a_src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ])
        .await;
    let peer_sequence = producer_device
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

    run_cycle_in_task(Arc::clone(&cycle_storage), device_m.clone(), ld.clone())
        .await
        .expect("M's pull cycle succeeds");
    assert_eq!(
        db_m.query_test_text("SELECT title FROM notes WHERE id = 'a1'")
            .await,
        "FromA",
        "M applied A's changeset",
    );

    // Cycle 2 has no host write. The applied row must not create a local data
    // commit because apply bypasses the host write ledger.
    let before = device_m
        .latest_local_store_position()
        .await
        .expect("read local Store position before the empty cycle");
    run_cycle_in_task(cycle_storage, device_m.clone(), ld.clone())
        .await
        .expect("M's empty cycle succeeds");
    let after = device_m
        .latest_local_store_position()
        .await
        .expect("read local Store position after the empty cycle")
        .expect("the empty cycle publishes its acknowledgement");
    let registration = crate::database::StoreDatabase::new(&db_m)
        .local_blob_write_authority()
        .await
        .expect("load local Store registration");
    let commit = device_m
        .load_commit_for_test(&after)
        .await
        .expect("load empty-cycle acknowledgement commit");
    assert_eq!(commit.author(), registration.value());
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
    let (_tmp, ld) = temp_store_dir();
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    let device = storage.open_into(&db).await.expect("open exact test Store");
    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let failed = match run_cycle_in_task(
        Arc::clone(&reject_blob_create),
        device.clone(),
        ld.clone(),
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
    let pending = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read retryable Store writes");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, crate::WriteStatus::Publishing);
    let rejected_blobs = reject_blob_create.rejected_blobs();
    assert_eq!(rejected_blobs.len(), 1);
    let prepared_blob = rejected_blobs[0].clone();
    let prepared = crate::database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write after blob failure")
        .expect("provider failure retains the exact prepared publication");
    assert_eq!(prepared.audiences.blobs.len(), 1);
    assert_eq!(prepared.audiences.blobs[0].blob(), &prepared_blob);
    assert!(
        storage
            .storage()
            .verify_blob_object(&prepared_blob)
            .await
            .is_err(),
        "the failed blob upload did not publish the blob"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("host blob retry cycle succeeds");
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the pending writes clear once the retry publishes"
    );
    let activated_blob = db
        .stored_blob_for_row("note_photos", "hponly")
        .await
        .expect("retry activates the exact row blob binding");
    assert_eq!(activated_blob, prepared_blob);
    storage
        .storage()
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry uploads and reads back the exact host-provided blob");
}

#[tokio::test]
async fn each_host_write_publishes_the_blob_facts_from_its_own_commit() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let blob_decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let db = open_test_db_with_blob(blob_decl.clone());
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    storage
        .retain_store_packages_for_assertion(&db, b"each-host-write-blob-facts")
        .await;
    let device = storage
        .open_into(&db)
        .await
        .expect("bind package writer device");
    db.execute_test_host_write(&format!(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01'); \
         INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('photo', 'n1', 'cover', 5, '{}', 'blob-a', \
                 '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"first"),
    ))
    .await;
    db.execute_test_host_write(&format!(
        "UPDATE note_photos \
             SET blob_id = 'blob-b', size = 6, hash = '{}', \
                 _updated_at = '0000000002000-0000-M' \
             WHERE id = 'photo'",
        coven_protocol::blob::content_hash(b"second"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "blob-a", b"first")
        .await
        .expect("store first write's blob");
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "blob-b", b"second")
        .await
        .expect("store second write's blob");

    let error = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::reject_ack_create(Arc::clone(
            &storage,
        ))),
        device,
        ld.clone(),
    )
    .await
    .expect_err("acknowledgement create stops the cycle before package reclamation");
    assert!(
        error
            .to_string()
            .contains("unexpected Store acknowledgement create"),
        "unexpected post-package failure: {error}"
    );
    assert!(db.latest_store_snapshot_meta().await.is_some());

    let stream_id = db.local_store_stream_id().await;
    let package_device = storage
        .bind_device(&db, &keypair)
        .await
        .expect("bind package inspection Store");
    let mut published_blob_ids = Vec::new();
    for seq in [1, 2] {
        let (commit_ref, _commit) = package_device
            .load_exact_materialized_commit(&stream_id, seq)
            .await
            .expect("load exact materialized commit")
            .expect("write has a commit");
        let package = package_device
            .load_store_package_for_test(&commit_ref)
            .await
            .expect("load exact Store package")
            .expect("commit has a package");
        let package = coven_protocol::audience_package::AudiencePackage::parse(&package.value)
            .expect("parse exact audience package");
        for binding in package.blob_bindings() {
            storage
                .storage()
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
    let (_tmp, ld) = temp_store_dir();
    // The live cipher is generation 1; the cloud has committed generation 2.
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");
    let write_id = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read rotation-paused Store write")
        .into_iter()
        .next()
        .expect("host write is queued")
        .write_id
        .clone();

    let device = storage.open_into(&db).await.expect("open exact test Store");
    device.mark_rotation_committed_for_test(2).unwrap();

    device
        .run_cycle(&ld, None)
        .await
        .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert!(
        db.pending_write_count().await > 0,
        "the host-blob changeset stays queued while sealing is paused",
    );
    let activated_bindings = db
        .test_sql(|database| {
            database.exact_row_blob_locator_count(
                "note_photos",
                "hponly",
                "id",
                "0000000001000-0000-M",
            )
        })
        .await
        .expect("count exact host-blob bindings");
    assert_eq!(
        activated_bindings, 0,
        "rotation pause installs no activated host-blob binding",
    );
    let exact_outbox_rows = db
        .test_sql(|database| {
            database.exact_upload_outbox_count(
                "note_photos",
                "hponly",
                "id",
                "0000000001000-0000-M",
            )
        })
        .await
        .expect("count exact host-blob upload handoffs");
    assert_eq!(
        exact_outbox_rows, 0,
        "rotation pause creates neither a cloud upload nor a Created handoff",
    );
    assert_eq!(
        coven_foundation::store_dir::StoreDir::read_local_blob(&ld, "photos", "hponly", 5)
            .await
            .expect("read rotation-paused local blob"),
        Some(b"cover".to_vec()),
        "the pending Store write retains its exact local blob source",
    );

    // Adoption clears the retained gate; the first cycle after publishes the
    // queued changeset and uploads its blob.
    device.clear_rotation_gate_for_test();
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the queued changeset publishes on the first cycle after adoption",
    );
    let published = match crate::database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read adopted Store write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("adopted Store write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(
        crate::database::StoreDatabase::new(&db)
            .exact_materialized_ref(&stream_id, published.coord.sequence())
            .await
            .expect("read adopted exact Store position")
            .is_some(),
        "the published Store write is materialized",
    );
    let activated = db
        .stored_blob_for_row("note_photos", "hponly")
        .await
        .expect("adoption activates the exact host-blob binding");
    storage
        .storage()
        .verify_blob_object(&activated)
        .await
        .expect("the activated host blob reads back exactly");
    assert_eq!(
        tokio::fs::read(
            &ld.cache_blob_path("photos", activated.locator().locator_hash())
                .expect("host-blob cache path"),
        )
        .await
        .expect("read adopted host-blob cache"),
        b"cover",
        "CacheEager policy retains the published blob in the evictable cache",
    );
    assert!(
        ld.read_local_blob("photos", "hponly", 5)
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
    let (_tmp, ld) = temp_store_dir();
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Release', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        crate::database::StoreDatabase::new(&db),
        ld.clone(),
    )
    .local_transitions()
    .make_remote("notes", "n1", false)
    .await
    .expect("queue the host-provided make_remote intent");

    let device = storage.open_into(&db).await.expect("open exact test Store");
    device.mark_rotation_committed_for_test(2).unwrap();

    device
        .run_cycle(&ld, None)
        .await
        .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert_eq!(
        db.query_test_text("SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'")
            .await,
        "0",
        "the make_remote gate does not flip while sealing is paused",
    );
    assert!(
        db.make_remote_intent_present("notes", "n1").await,
        "the make_remote intent stays queued while sealing is paused",
    );
    assert!(
        !storage
            .stored_blob_exists(&db, "note_photos", "hponly")
            .await,
        "no host-provided blob is sealed to the cloud while sealing is paused",
    );

    // Adoption clears the pause; the first cycle after completes the intent.
    device.clear_rotation_gate_for_test();
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        db.query_test_text("SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'")
            .await,
        "1",
        "the make_remote gate flips on the first cycle after adoption",
    );
    assert!(
        !db.make_remote_intent_present("notes", "n1").await,
        "completing the make_remote consumes its intent",
    );
    assert!(
        storage
            .stored_blob_exists(&db, "note_photos", "hponly")
            .await,
        "the host-provided blob uploads on the first cycle after adoption",
    );
}

#[tokio::test]
async fn ready_make_remote_provider_transport_is_offline() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage =
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('transport-root', 'Root', NULL, 0, \
                 '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('transport-blob', 'transport-root', 'cover', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        "photos",
        "transport-blob",
        b"cover",
    )
    .await
    .expect("store host-provided blob");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        crate::database::StoreDatabase::new(&db),
        ld.clone(),
    )
    .local_transitions()
    .make_remote("notes", "transport-root", false)
    .await
    .expect("queue make_remote intent");
    fail_exact_create_on(&storage, 1);
    let device = storage.open_into(&db).await.expect("open exact test Store");

    let failed = device
        .run_cycle(&ld, None)
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
    let (_tmp, ld) = temp_store_dir();
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    storage
        .retain_store_packages_for_assertion(&db, b"captured-changeset-blob-retry")
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('firstblob', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('secondblob', 'n1', 'cover', 6, '{}', '0000000001001-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"first"),
        coven_protocol::blob::content_hash(b"second"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "firstblob", b"first")
        .await
        .expect("store first host-provided blob");
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "secondblob", b"second")
        .await
        .expect("store second host-provided blob");

    let device = storage.open_into(&db).await.expect("open exact test Store");
    let reject_second_blob = Arc::new(CycleStorageInterceptor::reject_blob_create_on(
        Arc::clone(&storage),
        2,
    ));
    let failed = match run_cycle_in_task(
        Arc::clone(&reject_second_blob),
        device.clone(),
        ld.clone(),
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
        .storage()
        .verify_blob_object(&attempted_blobs[0])
        .await
        .expect("the first exact blob reached cloud before the second failed");
    assert!(storage
        .storage()
        .verify_blob_object(&attempted_blobs[1])
        .await
        .is_err());
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(&ld, "photos", "firstblob", 5)
            .await
            .expect("read first local")
            .is_some(),
        "the first local copy remains because the changeset was not published"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("two-blob retry cycle succeeds");
    let stream_id = db.local_store_stream_id().await;
    assert!(crate::database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id, 2)
        .await
        .expect("read retried exact materialized Store commit")
        .is_some());
    let activated_first = db
        .stored_blob_for_row("note_photos", "firstblob")
        .await
        .expect("retry activates the first exact blob binding");
    let activated_second = db
        .stored_blob_for_row("note_photos", "secondblob")
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
        .storage()
        .verify_blob_object(&activated_first)
        .await
        .expect("the first exact blob remains readable after retry");
    storage
        .storage()
        .verify_blob_object(&activated_second)
        .await
        .expect("the second exact blob is readable after retry");
}

#[tokio::test]
async fn already_uploaded_host_blob_publishes_without_local_copy_or_reupload() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    storage
        .retain_store_packages_for_assertion(&db, b"already-uploaded-host-blob")
        .await;
    let device = storage
        .open_into(&db)
        .await
        .expect("bind local Store device");
    let pass_through = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('remoteonly', 'n1', 'cover', 15, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"already durable"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        "photos",
        "remoteonly",
        b"already durable",
    )
    .await
    .expect("store the first publication's host-provided blob");

    run_cycle_in_task(Arc::clone(&pass_through), device.clone(), ld.clone())
        .await
        .expect("first host blob cycle succeeds");
    let stream_id = db.local_store_stream_id().await;
    assert!(crate::database::StoreDatabase::new(&db)
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
        .storage()
        .verify_blob_object(&published_blob)
        .await
        .expect("read back the first exact remote blob object");
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(&ld, "photos", "remoteonly", 15)
            .await
            .expect("read cache-lazy host blob after publication")
            .is_none(),
        "the first publication removes the cache-lazy local copy",
    );
    db.execute_test_host_write(
        "UPDATE note_photos \
         SET _updated_at = '0000000002000-0000-M' \
         WHERE id = 'remoteonly'",
    )
    .await;

    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(Arc::clone(&reject_blob_create), device, ld.clone())
        .await
        .expect("already-uploaded host blob cycle succeeds");
    assert!(crate::database::StoreDatabase::new(&db)
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
        .storage()
        .verify_blob_object(&republished_blob)
        .await
        .expect("read back the re-emitted exact remote blob object");
}

#[tokio::test]
async fn fresh_push_failure_keeps_cache_lazy_local_copy_until_retry_publishes() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let (db, storage) = blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let device = storage
        .open_into(&db)
        .await
        .expect("bind local Store device");
    let device_id = device.device_id.clone();
    storage
        .retain_store_packages_for_assertion(&db, b"fresh-push-retry")
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('lazyblob', 'n1', 'cover', 4, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"lazy"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "lazyblob", b"lazy")
        .await
        .expect("store cache-lazy host-provided blob");
    let pending = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read pending Store write");
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
    let error = run_cycle_in_task(Arc::clone(&cycle_storage), device.clone(), ld.clone())
        .await
        .expect_err("the first Store package append fails");
    assert_eq!(
        error.to_string(),
        "publish Store write: storage operation failed: InMemoryCloudHome: forced failure before exact create call 1",
        "cycle surfaces the exact Store package append failure",
    );
    let prepared = crate::database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read outbound Store queue")
        .expect("the exact prepared Store write remains durable");
    assert_ne!(
        prepared.commit.value.write_id, write_id,
        "the failed predecessor remains prepared ahead of the blob write",
    );
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local")
            .is_some(),
        "the local copy remains until the changeset is published"
    );

    run_cycle_in_task(cycle_storage, device, ld.clone())
        .await
        .expect("prepared Store write retry succeeds");
    let status = crate::database::StoreDatabase::new(&db)
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
    let stream_id = db.local_store_stream_id().await;
    let published = crate::database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id, commit.coord.sequence())
        .await
        .expect("read retried exact Store position")
        .expect("retried exact Store position is materialized");
    assert_eq!(published, commit);
    let device = storage
        .bind_device(&db, &keypair)
        .await
        .expect("bind retried Store writer");
    let (published_ref, published_commit) = device
        .load_exact_materialized_commit(&stream_id, commit.coord.sequence())
        .await
        .expect("load retried exact Store commit")
        .expect("retried exact Store commit exists");
    assert_eq!(published_ref, published);
    assert_eq!(published_commit.write_id, write_id);
    assert!(
        published_commit.store_package().is_some(),
        "the blob Store write carries an exact Store package reference",
    );
    let activated_blob = db
        .stored_blob_for_row("note_photos", "lazyblob")
        .await
        .expect("retry activates the exact cache-lazy blob binding");
    storage
        .storage()
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry leaves the exact cache-lazy blob readable");
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local after publish")
            .is_none(),
        "the local copy drops after the prepared write retry commits"
    );
}

/// The main-push and post-pull paths stamp the acknowledgement with an RFC 3339
/// `last_sync`.
#[tokio::test]
async fn push_cycle_writes_rfc3339_ack_timestamp() {
    let db = open_test_db();
    let keypair = UserKeypair::generate();
    let storage =
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    storage
        .retain_store_packages_for_assertion(&db, b"push-cycle-head-timestamp")
        .await;

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read push-timestamp write")
        .into_iter()
        .next()
        .expect("push-timestamp write is pending")
        .write_id;

    let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
    cycle_device
        .run_cycle(&ld, None)
        .await
        .expect("run acknowledgement timestamp cycle");
    let published = match crate::database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read push-timestamp write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("push-timestamp write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(crate::database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id, published.coord.sequence())
        .await
        .expect("read push-timestamp materialization")
        .is_some());
    storage.assert_latest_ack_timestamp_is_rfc3339(&db).await;
}

/// Snapshot metadata records its creation time as RFC 3339.
#[tokio::test]
async fn snapshot_cycle_writes_rfc3339_metadata_timestamp() {
    let keypair = UserKeypair::generate();
    let db = open_test_db();
    let storage =
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");

    let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
    cycle_device
        .run_cycle(&ld, None)
        .await
        .expect("run snapshot timestamp cycle");
    let snapshot = db
        .latest_store_snapshot_meta()
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
        let storage =
            cycle_test_store(&db, &owner, crate::sync::test_helpers::test_cloud_home()).await;
        let local_device = storage
            .founder_device()
            .await
            .expect("retain cadence Store device");
        let source = open_test_db();
        let changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('cadence', 'Cadence', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
            ])
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
        storage.pull_into(&db, &peer_store_dir).await;
        let local_at_snapshot = local_device
            .latest_local_store_position()
            .await
            .expect("read local Store position after peer setup")
            .expect("local Store stream has an exact snapshot position");
        let local_snapshot_sequence = local_at_snapshot.coord.sequence();
        let local_stream = local_at_snapshot.coord.stream_id;
        let peer_stream = peer_at_snapshot.coord.stream_id;
        let snapshot_device = storage
            .open_into(&db)
            .await
            .expect("open Store before publishing cadence snapshot");
        let mut snapshot_writer = snapshot_device
            .authorize_writer()
            .await
            .expect("authorize cadence snapshot writer");
        snapshot_writer
            .push_store_snapshot(
                crate::database::CreatedSnapshot {
                    db_image: b"cadence-snapshot".to_vec(),
                    blobs: Vec::new(),
                },
                coven_protocol::store_commit::CommitFrontier(BTreeMap::from([
                    (local_stream, local_at_snapshot),
                    (peer_stream, peer_at_snapshot),
                ])),
                db.schema_version(),
                T0.to_string(),
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
            local_device
                .latest_local_store_position()
                .await
                .expect("read latest local Store commit")
                .expect("local Store stream has commits")
                .coord
                .sequence(),
            local_after_snapshot,
        );

        let unregistered_member = UserKeypair::generate();
        storage
            .invite_member(
                &db,
                &owner,
                &pubkey_hex(&unregistered_member),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Test Store",
            )
            .await
            .expect("invite unregistered member to hold back package reclamation");

        let (_temp, store_dir) = temp_store_dir();
        let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
        cycle_device
            .run_cycle(&store_dir, None)
            .await
            .expect("run snapshot cadence cycle");

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
        let storage =
            cycle_test_store(&db, &owner, crate::sync::test_helpers::test_cloud_home()).await;
        let source = open_test_db();
        let first = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-1', 'First', NULL, 1, \
                         '0000000001000-0000-source', '2026-01-01')",
            ])
            .await;
        let second = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-2', 'Second', NULL, 1, \
                         '0000000002000-0000-source', '2026-01-01')",
            ])
            .await;

        let at_snapshot = storage
            .publish_changeset("local", 1, &first, SCHEMA_VERSION)
            .await
            .expect("publish Store commit before snapshot");
        let snapshot_device = storage
            .open_into(&db)
            .await
            .expect("open Store before publishing timed snapshot");
        let mut snapshot_writer = snapshot_device
            .authorize_writer()
            .await
            .expect("authorize timed snapshot writer");
        snapshot_writer
            .push_store_snapshot(
                crate::database::CreatedSnapshot {
                    db_image: b"time-cadence-snapshot".to_vec(),
                    blobs: Vec::new(),
                },
                coven_protocol::store_commit::CommitFrontier(BTreeMap::from([(
                    at_snapshot.coord.stream_id,
                    at_snapshot,
                )])),
                db.schema_version(),
                T0.to_string(),
            )
            .await
            .expect("publish timed snapshot");
        storage
            .publish_changeset("local", 2, &second, SCHEMA_VERSION)
            .await
            .expect("publish one Store commit after snapshot");

        let now = chrono::DateTime::parse_from_rfc3339("2024-01-02T01:00:00Z")
            .expect("parse timed snapshot clock")
            .with_timezone(&chrono::Utc);
        let (_temp, store_dir) = temp_store_dir();
        snapshot_device
            .run_cycle_with(&FixedClock(now), None, &store_dir, None)
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
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let (_tmp, ld) = temp_store_dir();
    storage
        .retain_store_packages_for_assertion(&db, b"prepared-retry-head-timestamp")
        .await;
    let device = storage
        .open_into(&db)
        .await
        .expect("bind prepared-retry device");

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // The first push fails at the package append, so the prepared write remains
    // owned by its durable record and no head is written for it yet.
    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device.clone(),
        ld.clone(),
    )
    .await
    .expect_err("the first Store package append fails");
    assert!(
        crate::database::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact Store batch remains durable after append failure",
    );

    // The next cycle retries the prepared write.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld,
    )
    .await
    .expect("retry prepared Store write");
    assert!(crate::database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read retried outbound Store queue")
        .is_none());
    storage.assert_latest_ack_timestamp_is_rfc3339(&db).await;
}

#[tokio::test]
async fn missing_user_blob_blocks_prepared_write_before_publish() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    let device = storage.open_into(&db).await.expect("open exact test Store");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let planted = storage
        .create_exact_opaque_blob("audio", "audio1", b"AUDIO")
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('audio1', 'n1', 'audio', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"AUDIO"),
    ))
    .await;

    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(Arc::clone(&cycle_storage), device.clone(), ld.clone())
        .await
        .expect_err("the first Store package append fails");
    let first_write_id = crate::database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write")
        .expect("the exact Store write remains after append failure")
        .commit
        .value
        .write_id
        .clone();
    assert!(!storage.local_store_package_exists(&db, 2).await);

    storage
        .storage()
        .delete_blob_object(&planted)
        .await
        .expect("delete exact user-provided blob");
    let retry = run_cycle_in_task(Arc::clone(&cycle_storage), device.clone(), ld.clone()).await;
    let err = match retry {
        Err(err) => err,
        Ok(_) => panic!("prepared write must recheck the remote user-provided blob"),
    };

    assert!(
        err.to_string()
            .contains("prepare Store write: outbound blob audio/audio1 is absent from storage"),
        "prepared write surfaces the missing blob: {err}",
    );
    let first_write_status = crate::database::StoreDatabase::new(&db)
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
    let pending = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read pending writes");
    assert_eq!(pending.len(), 1);
    let blocked_write_id = pending[0].write_id.clone();
    let blocked = crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
        namespace: "audio".to_string(),
        id: "audio1".to_string(),
    });
    assert_eq!(pending[0].status, blocked);
    assert!(
        !storage.local_store_package_exists(&db, 2).await,
        "the blocked write has no package or head",
    );

    let _restored = storage
        .create_exact_opaque_blob("audio", "audio1", b"AUDIO")
        .await;
    run_cycle_in_task(cycle_storage, device, ld.clone())
        .await
        .expect("restored missing user blob cycle succeeds");
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .write_status(&blocked_write_id)
            .await
            .expect("read blocked write status"),
        blocked,
        "a semantic block is not retried by reconnect",
    );
    assert!(!storage.local_store_package_exists(&db, 2).await);
}

#[tokio::test]
async fn outgoing_preparation_failure_keeps_pending_write_for_retry() {
    let keypair = UserKeypair::generate();
    let (_tmp, ld) = temp_store_dir();
    let db = open_test_db();
    let storage = Arc::new(
        cycle_test_store(&db, &keypair, crate::sync::test_helpers::test_cloud_home()).await,
    );
    storage
        .retain_store_packages_for_assertion(&db, b"outgoing-preparation-retry")
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('prepare-fail', 'Prepare Fail', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = crate::database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read preparation-failure write")
        .into_iter()
        .next()
        .expect("preparation-failure write is pending")
        .write_id;

    db.test_sql(|database| database.install_outbound_preparation_failure_trigger())
        .await
        .expect("install Store preparation fault");
    let device = storage.open_into(&db).await.expect("open exact test Store");
    let failed = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device.clone(),
        ld.clone(),
    )
    .await
    .expect_err("outgoing preparation should fail");
    assert!(
        failed.contains("injected Store preparation failure"),
        "cycle surfaces the outgoing preparation failure: {failed}"
    );
    assert_eq!(
        db.pending_write_count().await,
        1,
        "the pending write remains queued when outgoing preparation fails"
    );
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .write_status(&write_id)
            .await
            .expect("read failed-preparation write status"),
        crate::WriteStatus::Pending,
    );

    db.test_sql(|conn| {
        conn.execute_batch("DROP TRIGGER fail_outbound_preparation")
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("remove Store preparation fault");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("retry outgoing preparation");
    let published = match crate::database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read retried preparation write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("retried preparation write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(crate::database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id, published.coord.sequence())
        .await
        .expect("read retried preparation materialization")
        .is_some());
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the pending write leaves the pending set after publication"
    );
}

struct InterceptedCycle<'a> {
    storage: &'a CycleStorageInterceptor,
    device: TestDevice,
    store_dir: &'a StoreDir,
}

impl<'a> InterceptedCycle<'a> {
    fn new(
        storage: &'a CycleStorageInterceptor,
        device: TestDevice,
        store_dir: &'a StoreDir,
    ) -> Self {
        Self {
            storage,
            device,
            store_dir,
        }
    }

    async fn run(&self) {
        self.storage
            .run_sync_cycle(&self.device, self.store_dir)
            .await
            .expect("cycle");
    }
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
    let storage = Arc::new(
        cycle_test_store(
            &db_m,
            &keypair,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await,
    );
    let (_tmp, ld) = temp_store_dir();

    // Peer A's changeset 1 (a shareable note).
    let a_src = open_test_db();
    let a_cs = a_src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ])
        .await;
    let published = storage
        .publish_changeset("A", 1, &a_cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let published_stream = published.coord.stream_id;
    let stream_id = published_stream.to_string();

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    let device = storage
        .open_into(&db_m)
        .await
        .expect("bind retained-replay Store device");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
        ld.clone(),
    )
    .await
    .expect("retained-replay cycle succeeds");

    let snapshot = db_m
        .latest_store_snapshot_meta()
        .await
        .expect("the reclamation cycle publishes a covering snapshot");
    assert!(matches!(
        &snapshot.coverage,
        coven_protocol::store_commit::CommitFrontier(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    let ack_ref = store_database(&db_m)
        .latest_local_store_ack()
        .await
        .expect("read reclamation acknowledgement")
        .expect("the reclamation cycle publishes an acknowledgement")
        .reference;
    let device = storage
        .bind_device(&db_m, &keypair)
        .await
        .expect("bind reclamation acknowledgement Store");
    let local_device = db_m
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registrations = crate::database::StoreDatabase::new(&db_m)
        .activated_store_device_registration_records()
        .await
        .expect("read reclamation Store registrations");
    assert_eq!(registrations.len(), 1, "{registrations:?}");
    let registration = registrations
        .into_iter()
        .find(|registration| registration.value().device_id.to_string() == local_device)
        .expect("local reclamation Store registration is active");
    let acknowledgement = device
        .load_store_ack_for_test(&ack_ref, registration.value())
        .await
        .expect("load exact reclamation acknowledgement");
    assert!(matches!(
        &acknowledgement.store_cut,
        coven_protocol::store_commit::StoreHistoryCut(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    assert!(
        storage.store_package_exists(&db_m, &stream_id, 1).await,
        "the accepted Merge materialization retains its Store package for replay",
    );
}

/// Reclamation refuses the snapshot proof while one exact active device is
/// behind it. The behind device acknowledges the first data commit, the owner
/// publishes another, and both packages remain available for its later pull.
#[tokio::test]
async fn cycle_preserves_packages_until_every_device_covers_the_snapshot() {
    Box::pin(async {
        let owner = UserKeypair::generate();
        let owner_db = open_test_db();
        let storage = cycle_test_store(
            &owner_db,
            &owner,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let owner_device = storage
            .founder_device()
            .await
            .expect("retain Owner Store device");
        let (_tmp, ld) = temp_store_dir();
        let source = open_test_db();
        let first_changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a1', 'Title Alpha', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
            ])
            .await;
        storage
            .publish_changeset("owner", 1, &first_changeset, SCHEMA_VERSION)
            .await
            .expect("publish first exact Store changeset");

        let behind = UserKeypair::generate();
        storage
            .invite_member(
                &owner_db,
                &owner,
                &pubkey_hex(&behind),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Test Store",
            )
            .await
            .expect("invite exact behind Member identity");
        let behind_db = open_test_db();
        let behind_store = storage
            .activate_joined_device(&owner_db, &behind_db, &behind, T0)
            .await
            .expect("activate exact joined test device");
        behind_store
            .pull_store(&ld)
            .await
            .expect("pull initial behind Member Store state");

        let behind_frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::database::StoreDatabase::new(&behind_db)
                .materialized_frontier()
                .await
                .expect("read behind device frontier"),
        )
        .expect("validate behind device frontier");
        behind_store
            .stage_acknowledgement(behind_frontier, T0.to_string())
            .await
            .expect("stage behind device acknowledgement");
        behind_store
            .drain_acknowledgements()
            .await
            .expect("publish behind device acknowledgement");

        let second_changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a2', 'Title Beta', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
            ])
            .await;
        let second_sequence = owner_device
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
            let registration = crate::database::StoreDatabase::new(&owner_db)
                .local_blob_write_authority()
                .await
                .expect("read owner announcement authority");
            let owner_stream = registration
                .value()
                .store_announcement_activation(registration.reference())
                .expect("derive owner Store announcement activation")
                .author_stream_id()
                .to_string();
            let second_sequence = owner_device
                .latest_local_store_position()
                .await
                .expect("read published owner Store position")
                .expect("second owner Store commit is materialized")
                .coord
                .sequence();
            let cycle_device = storage
                .open_into(&owner_db)
                .await
                .expect("open exact test Store");
            cycle_device
                .run_cycle(&ld, None)
                .await
                .expect("run package-retention cycle");

            assert!(
                storage
                    .store_package_exists(&owner_db, &owner_stream, 1)
                    .await,
                "reclamation keeps the earlier package while an active device is behind",
            );
            assert!(
                storage
                    .store_package_exists(&owner_db, &owner_stream, second_sequence)
                    .await,
                "reclamation keeps the package the behind device still needs",
            );

            behind_store
                .pull_store(&ld)
                .await
                .expect("pull retained changeset into behind Member Store");
            assert!(
                behind_db
                    .test_row_exists("SELECT 1 FROM notes WHERE id = 'a2'")
                    .await,
                "the behind device pulls the retained changeset",
            );
        })
        .await
        .expect("snapshot coverage reclamation cycle task");
    })
    .await;
}

/// A registered Member publishes rows but cannot author a catalog snapshot.
#[tokio::test]
async fn member_device_does_not_create_a_snapshot() {
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = Arc::new(
        cycle_test_store(
            &owner_db,
            &owner,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await,
    );
    let (_tmp, ld) = temp_store_dir();
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    invite_test_member(&storage, &owner_db, &owner, &member, &encryption).await;

    let member_db = open_test_db();
    let member_device = storage
        .activate_joined_device(&owner_db, &member_db, &member, T0)
        .await
        .expect("activate exact joined test device");
    member_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-member', '2026-01-01')",
        )
        .await;

    let member_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    run_cycle_in_task(member_storage, member_device, ld.clone())
        .await
        .expect("Member Store cycle succeeds");

    assert!(
        storage.local_store_package_exists(&member_db, 1).await,
        "the Member's row publishes through its exact Store stream",
    );
    assert!(
        member_db.latest_store_snapshot_meta().await.is_none(),
        "a Member device cannot author catalog snapshot metadata",
    );
}

#[tokio::test]
async fn pull_refreshes_snapshot_authority_before_publication() {
    use coven_protocol::membership::MemberRole;

    let founder = UserKeypair::generate();
    let founder_db = open_test_db();
    let storage = cycle_test_store(
        &founder_db,
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let successor_owner = UserKeypair::generate();
    let encryption = EncryptionService::from_key([64; 32]);
    storage
        .invite_member(
            &founder_db,
            &founder,
            &pubkey_hex(&successor_owner),
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("invite successor Owner");
    let successor_db = open_test_db();
    storage
        .activate_joined_device(
            &founder_db,
            &successor_db,
            &successor_owner,
            "2026-07-26T00:00:00Z",
        )
        .await
        .expect("activate successor Owner device");
    storage
        .promote_active_member_fixture(
            &founder_db,
            &successor_db,
            &founder,
            &successor_owner,
            &encryption,
        )
        .await
        .expect("promote successor Owner");

    let founder_store = storage
        .bind_device(&founder_db, &founder)
        .await
        .expect("load founder Store");
    let mut authorized = founder_store
        .authorize_writer()
        .await
        .expect("authorize founder before removal");

    let custody = TestCustody::default();
    storage
        .remove_member(
            &successor_db,
            &successor_owner,
            &pubkey_hex(&founder),
            &encryption,
            &custody,
        )
        .await
        .expect("remove founder after cycle authorization");

    authorized
        .pull(Some(&encryption))
        .await
        .expect("pull founder removal");
    authorized
        .publish_due_snapshots("2026-07-26T01:00:00Z", Some(&encryption), false)
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
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let encryption = EncryptionService::from_key([43; 32]);
    invite_test_member(&storage, &owner_db, &owner, &member, &encryption).await;

    let member_db = open_test_db();
    storage
        .activate_joined_device(&owner_db, &member_db, &member, T0)
        .await
        .expect("activate exact joined test device");

    assert!(
        crate::database::StoreDatabase::new(&member_db)
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the public join sequence activates the joining registration",
    );
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
        storage: &'storage TestStore,
        owner: &UserKeypair,
        member: &UserKeypair,
    ) -> Self {
        invite_test_member(
            storage,
            owner_db,
            owner,
            member,
            &EncryptionService::from_key([59; 32]),
        )
        .await;
        let owner_device = storage
            .bind_device(owner_db, owner)
            .await
            .expect("bind owner Store");
        let pending_dir = tempfile::tempdir().expect("create join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
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

#[tokio::test]
async fn owner_accepts_access_activation_covered_by_a_later_predecessor_head() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut first =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let first_registration_request = first
        .pending_join
        .prepare_registration_request(first.approval)
        .await
        .expect("prepare first registration request");

    let second_member = UserKeypair::generate();
    let second =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &second_member).await;

    second
        .owner
        .accept_device_registration_request(first_registration_request)
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
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut approval =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let request = approval
        .pending_join
        .prepare_registration_request(approval.approval)
        .await
        .expect("prepare the joining device's registration request");

    // A row of the owner's own, queued as a Store write: the sync loop now
    // holds a head addressed to the same position the acceptance composes
    // against.
    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('queued', 'queued by the owner', NULL, 1, \
         '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let (_write_dir, store_dir) = temp_store_dir();
    {
        let store = crate::sync::store::Store::load(
            crate::database::StoreDatabase::new(&owner_db),
            storage.storage(),
            store_dir.clone(),
            owner.clone(),
        )
        .await
        .expect("load the owner's Store");
        let mut writer = store
            .authorize_writer()
            .await
            .expect("authorize the owner's registered writer");
        assert!(
            writer
                .prepare_pending_store_write()
                .await
                .expect("prepare the owner's queued Store write"),
            "the owner's row queues a Store write for the sync loop to publish",
        );
    }

    let mut test_points = owner_db.observe_test_points();
    let (position_held, resume_acceptance) =
        owner_db.arm_test_pause(crate::database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld);
    let accept_store = approval.owner.clone();
    let acceptance = tokio::spawn(async move {
        accept_store
            .accept_device_registration_request(request)
            .await
    });

    // Hold the acceptance exactly where it has read the position and not yet
    // published the head that takes it — the window the sync loop used to
    // publish into.
    position_held.notified().await;
    let drain_db = owner_db.clone();
    let drain_storage = storage.clone();
    let drain_store_dir = store_dir;
    let drain = tokio::spawn(async move {
        let store = crate::sync::store::Store::load(
            crate::database::StoreDatabase::new(&drain_db),
            drain_storage.storage(),
            drain_store_dir,
            owner.clone(),
        )
        .await
        .expect("load the owner's Store");
        let mut writer = store
            .authorize_writer()
            .await
            .expect("authorize the owner's registered writer");
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
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut fixture =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let request = fixture
        .pending_join
        .prepare_registration_request(fixture.approval)
        .await
        .expect("prepare exact registration request");
    let offer = request.approval.request.offer.as_ref();
    let plan = fixture
        .owner
        .prepare_operation_plan_for_test()
        .await
        .expect("prepare exact Owner Store commit");
    let cut = plan.predecessor_cut().expect("load exact predecessor cut");
    let membership = plan.membership_state().clone();
    let owner_authority = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let mut invalid_approval = request.approval.as_ref().clone();
    invalid_approval.corrupt_signature_for_test();
    let attempt = owner_authority
        .sign_device_join_attempt_for_test(
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
            offer.owner_grant.clone(),
        )
        .expect("Owner signs the attempt envelope");
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let prefix =
        coven_protocol::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
    let prepared = storage
        .storage()
        .prepare_protocol_object(
            &context,
            offer.attempt_slot.clone(),
            &prefix,
            attempt.to_bytes(),
        )
        .expect("prepare exact attempt object");
    storage
        .storage()
        .create_protocol_object(&prepared)
        .await
        .expect("publish exact attempt object");
    let attempt_ref = coven_protocol::store_commit::DeviceJoinAttemptRef {
        attempt_id: offer.attempt_id,
        attempt_hash: attempt.attempt_hash(),
        object: prepared.reference().clone(),
    };
    fixture
        .owner
        .verify_device_join_attempt_for_test(&attempt_ref, owner_authority.registration())
        .await
        .expect_err("the complete attempt loader rejects the embedded approval signature");
}

#[tokio::test]
async fn owner_rejects_invalid_access_activation_without_consuming_the_join_journal() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut fixture =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let valid_request = fixture
        .pending_join
        .prepare_registration_request(fixture.approval)
        .await
        .expect("prepare exact registration request");
    let mut invalid_access = valid_request.approval.access_grant.clone();
    invalid_access.activation.commit_hash =
        coven_protocol::store_commit::ObjectHash::digest(b"absent provider-access activation");
    let owner_authority = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let malformed_approval = owner_authority
        .sign_provider_admission_approval_without_shape_validation_for_test(
            valid_request.approval.request.as_ref().clone(),
            invalid_access,
            valid_request.approval.admission.clone(),
        );
    let malformed_request =
        coven_protocol::store_commit::device_join_exchange::DeviceRegistrationRequest::signed(
            malformed_approval,
            valid_request.expected_registration.clone(),
            valid_request.registration_slot.clone(),
            valid_request.response.clone(),
            &member,
        )
        .expect("joiner signs malformed remote request fixture");
    fixture
        .owner
        .accept_device_registration_request(malformed_request)
        .await
        .expect_err("Owner rejects the absent exact provider-access activation");
    fixture
        .owner
        .accept_device_registration_request(valid_request)
        .await
        .expect("valid retry remains possible after rejected activation");
}

#[tokio::test]
async fn joiner_rejects_access_commit_beyond_another_streams_exclusion_cutoff() {
    Box::pin(async {
        let founder = UserKeypair::generate();
        let founder_db = open_test_db();
        let storage = cycle_test_store(
            &founder_db,
            &founder,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let excluding_owner = UserKeypair::generate();
        let encryption = EncryptionService::from_key([62; 32]);
        storage
            .invite_member(
                &founder_db,
                &founder,
                &pubkey_hex(&excluding_owner),
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Test Store",
            )
            .await
            .expect("invite second exact Owner identity");
        let excluding_db = open_test_db();
        storage
            .activate_joined_device(
                &founder_db,
                &excluding_db,
                &excluding_owner,
                "2026-07-20T00:00:00Z",
            )
            .await
            .expect("activate second Owner device");
        storage
            .promote_active_member_fixture(
                &founder_db,
                &excluding_db,
                &founder,
                &excluding_owner,
                &encryption,
            )
            .await
            .expect("promote active second Owner");
        let founder_authority = storage
            .founder_device_authority()
            .await
            .expect("load exact founder authority");
        let excluding_store = storage
            .bind_device(&excluding_db, &excluding_owner)
            .await
            .expect("load excluding Owner Store");
        let proposal = match excluding_store
            .propose_device_exclusion(founder_authority.registration_ref())
            .await
            .expect("propose founder device exclusion")
        {
            crate::sync::store::StoreDeviceExclusionResult::ProposalActivated {
                proposal, ..
            } => proposal,
            result => panic!("unexpected exclusion proposal result: {result:?}"),
        };

        let joining_member = UserKeypair::generate();
        let mut approval =
            SamePrincipalApprovalFixture::prepare(&founder_db, &storage, &founder, &joining_member)
                .await;

        let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::database::StoreDatabase::new(&excluding_db)
                .materialized_frontier()
                .await
                .expect("load exclusion frontier"),
        )
        .expect("shape exclusion frontier");
        excluding_store
            .stage_acknowledgement(frontier, "2026-07-20T00:01:00Z".to_string())
            .await
            .expect("stage exclusion acknowledgement");
        excluding_store
            .drain_acknowledgements()
            .await
            .expect("publish exclusion acknowledgement");
        match excluding_store
            .finalize_device_exclusion(&proposal)
            .await
            .expect("activate founder exclusion")
        {
            crate::sync::store::StoreDeviceExclusionResult::OutcomeActivated { .. } => {}
            result => panic!("unexpected exclusion outcome result: {result:?}"),
        }

        approval
            .pending_join
            .prepare_registration_request(approval.approval)
            .await
            .expect_err("the excluded founder suffix cannot authorize provider access");
    })
    .await;
}

#[tokio::test]
async fn authenticated_next_head_with_a_missing_commit_body_rejects_provider_access() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut fixture =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let later_member = UserKeypair::generate();
    let later =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &later_member).await;
    storage
        .storage()
        .delete_protocol_object(&later.approval.access_grant.activation.object)
        .await
        .expect("remove the commit body behind its authenticated head");

    fixture
        .pending_join
        .prepare_registration_request(fixture.approval)
        .await
        .expect_err("an authenticated head cannot hide its missing commit body");
}

#[tokio::test]
async fn unauthenticated_next_head_does_not_hide_the_prior_accepted_access_commit() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut fixture =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let activation = fixture.approval.access_grant.activation.clone();
    let owner_authority = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let (next_slot, _) = fixture
        .owner
        .exact_next_announcement_slot_for_test(
            owner_authority.registration_ref(),
            owner_authority.registration(),
            Some(&activation),
        )
        .await
        .expect("load exact next announcement slot");
    let next_sequence = activation
        .coord
        .sequence()
        .checked_add(1)
        .expect("next sequence exists");
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::StoreHead,
    );
    let prefix = coven_protocol::store_commit::head_slot_prefix(
        &owner_authority.registration().device_id.to_string(),
        next_sequence,
    );
    let garbage = storage
        .storage()
        .prepare_protocol_object(&context, next_slot, &prefix, b"not a signed head".to_vec())
        .expect("prepare unauthenticated next-head bytes");
    storage
        .storage()
        .create_protocol_object(&garbage)
        .await
        .expect("publish unauthenticated next-head bytes");
    fixture
        .pending_join
        .prepare_registration_request(fixture.approval)
        .await
        .expect("unauthenticated garbage leaves the prior accepted access commit current");
}

#[tokio::test]
async fn authenticated_malformed_next_head_rejects_prior_provider_access() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let mut fixture =
        SamePrincipalApprovalFixture::prepare(&owner_db, &storage, &owner, &member).await;
    let activation = fixture.approval.access_grant.activation.clone();
    let owner_authority = storage
        .founder_device_authority()
        .await
        .expect("load exact founder authority");
    let (next_slot, accepted_head_ref) = fixture
        .owner
        .exact_next_announcement_slot_for_test(
            owner_authority.registration_ref(),
            owner_authority.registration(),
            Some(&activation),
        )
        .await
        .expect("load exact next announcement slot");
    let accepted_head_ref = accepted_head_ref.expect("activation has an accepted Store head");
    let accepted_head = fixture
        .owner
        .load_head_for_test(
            &accepted_head_ref,
            owner_authority.registration(),
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
    next_commit.coord = coven_protocol::store_commit::StoreCommitCoord {
        stream_id,
        sequence: next_sequence,
    };
    let stream_activation = owner_authority
        .registration()
        .store_announcement_activation(owner_authority.registration_ref())
        .expect("derive founder announcement activation")
        .activation_id();
    let malformed = owner_authority
        .sign_device_head_for_test(
            storage.root.store_root_hash,
            next_commit,
            accepted_head.history_summary,
            coven_protocol::store_commit::SuccessorLink {
                activation: stream_activation,
                predecessor: None,
                next_slot: next_slot.clone(),
            },
        )
        .expect("sign malformed successor chain");
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::StoreHead,
    );
    let prefix = coven_protocol::store_commit::head_slot_prefix(
        &owner_authority.registration().device_id.to_string(),
        next_sequence,
    );
    let prepared = storage
        .storage()
        .prepare_protocol_object(&context, next_slot, &prefix, malformed.to_bytes())
        .expect("prepare authenticated malformed head");
    storage
        .storage()
        .create_protocol_object(&prepared)
        .await
        .expect("publish authenticated malformed head");

    fixture
        .pending_join
        .prepare_registration_request(fixture.approval)
        .await
        .expect_err("an authenticated malformed successor makes current history unverifiable");
}

#[tokio::test]
async fn pre_attempt_device_join_abandonment_is_observed_and_retry_safe() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    let encryption = EncryptionService::from_key([44; 32]);
    invite_test_member(&storage, &owner_db, &owner, &member, &encryption).await;
    exercise_pre_attempt_abandonment(
        &crate::database::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn post_attempt_device_join_cancellation_closes_and_cleans_up_on_merge() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    invite_test_member(
        &storage,
        &owner_db,
        &owner,
        &member,
        &EncryptionService::from_key([45; 32]),
    )
    .await;
    exercise_post_attempt_cancellation(
        &crate::database::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
        JoinerCancellationDisposition::Closure,
    )
    .await;
}

#[tokio::test]
async fn missing_joiner_writes_are_revoked_and_cleaned_up_on_merge() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    invite_test_member(
        &storage,
        &owner_db,
        &owner,
        &member,
        &EncryptionService::from_key([46; 32]),
    )
    .await;
    exercise_post_attempt_cancellation(
        &crate::database::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
        JoinerCancellationDisposition::WriteRevocation,
    )
    .await;
}

#[tokio::test]
async fn cancellation_removes_an_inflight_registration_on_merge() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    invite_test_member(
        &storage,
        &owner_db,
        &owner,
        &member,
        &EncryptionService::from_key([52; 32]),
    )
    .await;
    let owner_database = crate::database::StoreDatabase::new(&owner_db);
    Box::pin(async {
        let owner_device = storage
            .bind_store_device(&owner_database, &owner)
            .await
            .expect("bind owner Store");
        let joining_db = open_test_db();
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner_device
            .begin_device_join(&pubkey_hex(&member))
            .await
            .expect("begin exact device join");
        let mut pending_join = storage
            .open_pending_device_join(&pending, &member, offer.clone())
            .await
            .expect("bind pending Store join");
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .expect("prepare exact provider access request");
        let approval = owner_device
            .authorize_device_provider_access(access_request, None)
            .await
            .expect("authorize exact provider access");
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .expect("prepare exact registration request");
        let provisional = owner_device
            .accept_device_registration_request(registration_request)
            .await
            .expect("activate exact join attempt");
        let provider_ready = owner_device
            .publish_device_provider_challenge(provisional.clone())
            .await
            .expect("publish same-principal provider readiness");
        let (_joining_store_dir_temp, joining_store_dir) = temp_store_dir();
        let mut joining_store = pending_join
            .begin_joining_store(
                crate::database::StoreDatabase::new(&joining_db),
                &joining_store_dir,
            )
            .await
            .expect("bind joining Store database");
        let (registration_visible, release_registration_create) =
            storage.pause_after_exact_create_call(1);
        let mut bootstrap = Box::pin(joining_store.bootstrap(provider_ready, T0));
        tokio::select! {
            () = registration_visible.notified() => {}
            result = &mut bootstrap => panic!(
                "bootstrap ended before reaching the registration create boundary: {result:?}"
            ),
        }
        let cancellation = owner_device
            .cancel_device_join(provisional.publication_authorization.attempt.clone())
            .await
            .expect("cancel while registration create is in flight");
        let administrator = owner_device
            .close_device_provider_admission(cancellation.clone())
            .await
            .expect("close provider admission during late create");
        let mut cancellation_join = storage
            .pending_device_join_observation(&pending, &offer)
            .await
            .expect("bind concurrent pending Store join")
            .authorize_closure(&member);
        let joiner = cancellation_join
            .close(cancellation.clone())
            .await
            .expect("close joining device during late create");
        release_registration_create.notify_one();
        let bootstrap_result = bootstrap.await;
        assert!(
            bootstrap_result.is_err(),
            "a registration deleted by cancellation cannot complete bootstrap"
        );
        let receipt = owner_device
            .prepare_device_join_cleanup(cancellation, administrator, joiner)
            .await
            .expect("prepare cleanup after in-flight registration");
        owner_device
            .activate_device_join_cleanup(receipt)
            .await
            .expect("activate cleanup after in-flight registration");
    })
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_resumes_after_pre_visibility_failure_on_merge() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    invite_test_member(
        &storage,
        &owner_db,
        &owner,
        &member,
        &EncryptionService::from_key([49; 32]),
    )
    .await;
    exercise_provider_access_grant_create_interruption(
        &crate::database::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
        ExactCreateInterruption::BeforeVisibility,
    )
    .await;
}

#[tokio::test]
async fn provider_access_grant_create_settles_lost_response_on_merge() {
    let OwnerAndMember {
        owner,
        owner_db,
        storage,
        member,
    } = owner_and_member().await;
    invite_test_member(
        &storage,
        &owner_db,
        &owner,
        &member,
        &EncryptionService::from_key([50; 32]),
    )
    .await;
    exercise_provider_access_grant_create_interruption(
        &crate::database::StoreDatabase::new(&owner_db),
        &storage,
        &owner,
        &member,
        ExactCreateInterruption::AfterVisibility,
    )
    .await;
}

#[tokio::test]
async fn cross_principal_device_join_completes_on_the_runtime_stack() {
    use coven_protocol::objects::{
        ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, StoreProviderBinding,
    };

    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = TestStore::create(
        &owner_db,
        "cross-principal-test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home_with_binding(ResolvedProviderBinding {
            store: StoreProviderBinding::Dropbox {
                namespace_id: "shared-namespace".to_string(),
            },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::Dropbox {
                    account_id: "administrator-account".to_string(),
                },
            },
        }),
    )
    .await
    .expect("create exact cross-principal test Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([43; 32]);
    invite_test_member(&storage, &owner_db, &owner, &member, &encryption).await;

    let member_db = open_test_db();
    storage
        .install_cross_principal_device(
            crate::database::StoreDatabase::new(&member_db),
            &member,
            "member-account",
            T0,
        )
        .await
        .expect("complete cross-principal device join");

    assert!(
        crate::database::StoreDatabase::new(&member_db)
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
    let storage = cycle_test_store(&db, &owner, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    let cycle_device = storage.open_into(&db).await.expect("open exact test Store");
    cycle_device
        .run_cycle(&ld, None)
        .await
        .expect("run owner snapshot cycle");

    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "an owner device must author catalog snapshot metadata",
    );
}

#[tokio::test]
async fn malformed_durable_pending_rotation_blocks_session_reopen() {
    let directory = tempfile::tempdir().expect("pending-rotation database directory");
    let path = directory.path().join("store.sqlite3");
    let open = || {
        crate::database::Database::open(
            &path,
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "pending-rotation-reopen-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open pending-rotation database")
    };
    let home = crate::InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let encryption = coven_keys::encryption::EncryptionService::from_key([17; 32]);
    let db = open();
    let store_database = crate::database::StoreDatabase::new(&db);
    let (_blob_temp, store_dir) = temp_store_dir();
    let storage = crate::storage::CloudSyncStorage::new(
        Arc::new(home.clone()),
        crate::storage::CloudCipher::Encrypted(encryption.clone()),
        crate::storage::BlobPathScheme::Hashed,
        "pending-rotation-reopen",
        signer.clone(),
    )
    .expect("construct pending-rotation storage");
    let components = crate::sync::cycle::PreparedSyncComponents::prepare(
        store_database.clone(),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(store_database.clone(), store_dir.clone()),
        storage,
        signer.clone(),
        crate::sync::cycle::StoreInitialization::CreateStore,
        None,
    )
    .await
    .expect("prepare pending-rotation Store")
    .initialize()
    .await
    .expect("initialize pending-rotation Store");
    let root = store_database
        .local_store_root_ref()
        .await
        .expect("read pending-rotation Store root")
        .expect("pending-rotation Store root exists");
    db.set_protocol_state(
        coven_protocol::objects::ROTATION_GATE_STATE_KEY,
        "not-a-rotation-gate",
    )
    .await
    .expect("persist malformed pending rotation");
    drop(components);
    drop(db);

    let reopened = open();
    let storage = crate::storage::CloudSyncStorage::new(
        Arc::new(home),
        crate::storage::CloudCipher::Encrypted(encryption),
        crate::storage::BlobPathScheme::Hashed,
        "pending-rotation-reopen",
        signer.clone(),
    )
    .expect("reconstruct pending-rotation storage");
    let result = crate::sync::cycle::PreparedSyncComponents::prepare(
        crate::database::StoreDatabase::new(&reopened),
        store_dir.clone(),
        crate::sync::test_owner_graph::local_blob_access(
            crate::database::StoreDatabase::new(&reopened),
            store_dir,
        ),
        storage,
        signer,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(crate::sync::cycle::InitSyncError::PendingRotationRestore(_))
    ));
}
