//! Device admission driven entirely through the storage-mediated transport.
//!
//! These tests run device admission with every cross-device hand-off carried by
//! the transport's slots, over an in-memory cloud home both sides share.

use std::sync::Arc;
use std::time::Duration;

use super::test_runtime::on_a_deep_stack;
use coven_foundation::clock::SystemClock;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::ObjectSlot;
use coven_replication::sync::store::{
    DeviceJoinAction, DeviceJoinOfferBundle, DeviceJoinRole, DeviceJoinTransport,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportTiming,
};
use coven_replication::sync::test_helpers::*;
use coven_storage::cloud::{no_progress, ExactSlotStorage, ExactUpload, UploadControl};

/// Fast enough that the drivers hand off within a test, generous enough that a
/// loaded machine never trips the deadline.
fn timing() -> DeviceJoinTransportTiming {
    DeviceJoinTransportTiming {
        poll: Duration::from_millis(2),
        deadline: Duration::from_secs(60),
    }
}

fn never_cancelled() -> tokio::sync::watch::Receiver<bool> {
    tokio::sync::watch::channel(false).1
}

fn no_join_progress() -> coven_replication::sync::JoiningDeviceJoinProgressObserver {
    Arc::new(|_| {})
}

/// Everything the two sides of one join need: the owner's open Store over the
/// shared in-memory home, and a factory for the joining device's client.
struct TransportFixture {
    owner_store: TestDevice,
    owner_db: coven_database::Database,
    owner_database: coven_database::StoreDatabase,
    /// The owner's own `TestStore`, kept so a test can publish an ordinary Store
    /// commit of the owner's while a join is mid-flight.
    owner_test_store: std::sync::Arc<TestStore>,
    /// The owner store's storage handle, retained so borrowing transports can
    /// point into it.
    owner_storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    owner_store_dir: coven_foundation::store_dir::StoreDir,
    /// The owner's identity, kept so a test can admit further members.
    owner_keypair: UserKeypair,
    home: Arc<coven_storage::InMemoryCloudHome>,
    member_pubkey: String,
    admission: coven_replication::sync::MemberAdmission,
    layout: coven_foundation::store_dir::StoreLayout,
    tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    /// The cloud home the joining device sees. Same principal as the owner's
    /// in the ordinary case; a different account for the cross-principal one,
    /// which is what makes the admission run its provider probe.
    joiner_home: Arc<coven_storage::InMemoryCloudHome>,
    /// The provider-side grant a cross-principal admission needs, or `None`
    /// when both sides are the same principal and no sharing step exists.
    access_administrator:
        Option<coven_replication::sync::test_helpers::TestDropboxAccessAdministrator>,
    _app: tempfile::TempDir,
    _snapshot: tempfile::TempDir,
    _owner_store_tmp: tempfile::TempDir,
}

/// The Dropbox namespace a cross-principal fixture's store lives in.
const CROSS_PRINCIPAL_NAMESPACE: &str = "transport-shared-namespace";

impl TransportFixture {
    /// Owner and joiner on one provider account: the admission takes the
    /// same-principal path and publishes no probe.
    async fn build(store_id: &str) -> Self {
        Self::build_with_members(store_id, None, false).await.0
    }

    /// Owner and joiner on separate provider accounts sharing one Dropbox
    /// namespace: the admission takes the cross-principal path, so the
    /// provider probe travels through the transport with everything else.
    async fn build_cross_principal(store_id: &str) -> Self {
        Self::build_with_members(
            store_id,
            Some(coven_protocol::ProviderPrincipalId::Dropbox {
                account_id: "joining-device-account".to_string(),
            }),
            false,
        )
        .await
        .0
    }

    async fn build_two_joiners(store_id: &str) -> (Self, String) {
        let (fixture, second_member) = Self::build_with_members(store_id, None, true).await;
        (
            fixture,
            second_member.expect("two-joiner fixture creates its second member"),
        )
    }

    async fn build_with_members(
        store_id: &str,
        joiner_principal: Option<coven_protocol::ProviderPrincipalId>,
        add_second_member: bool,
    ) -> (Self, Option<String>) {
        coven_keys::keys::test_keyring::install();
        let owner = UserKeypair::generate();
        let (owner_store_tmp, owner_db_store_dir) = temp_store_dir();
        let owner_db = open_test_db(owner_db_store_dir.clone());
        let owner_database = coven_database::StoreDatabase::from_database(owner_db.clone());
        let create_store_db = owner_db.clone();
        let create_store_db_store_dir = owner_db_store_dir.clone();
        let create_store_owner = owner.clone();
        let store_id_owned = store_id.to_string();
        let cross_principal = joiner_principal.is_some();
        let home = if cross_principal {
            test_cloud_home_with_binding(coven_protocol::ResolvedProviderBinding {
                store: coven_protocol::StoreProviderBinding::Dropbox {
                    namespace_id: CROSS_PRINCIPAL_NAMESPACE.to_string(),
                },
                device: coven_protocol::ProviderDeviceBinding {
                    principal: coven_protocol::ProviderPrincipalId::Dropbox {
                        account_id: "owner-device-account".to_string(),
                    },
                },
            })
        } else {
            test_cloud_home()
        };
        let create_store_home = home.clone();
        let fixture = tokio::spawn(async move {
            TestStore::create_with_connection(
                &create_store_db,
                create_store_db_store_dir,
                &store_id_owned,
                create_store_owner,
                create_store_home,
            )
            .await
        })
        .await
        .expect("Store creation task")
        .expect("create Owner Store");
        let (store, owner_storage) = fixture;
        let joining_identity =
            coven_keys::keys::mint_pending_identity().expect("mint pending joining identity");
        let member_pubkey = coven_keys::keys::public_key_hex(&joining_identity);
        let admission = store
            .admit_member(
                &owner_db,
                owner_db_store_dir.clone(),
                &owner,
                &member_pubkey,
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Device Join Transport Store",
            )
            .await
            .expect("admit joining identity");
        let second_member_pubkey = if add_second_member {
            let identity =
                coven_keys::keys::mint_pending_identity().expect("mint second joining identity");
            let pubkey = coven_keys::keys::public_key_hex(&identity);
            store
                .admit_member(
                    &owner_db,
                    owner_db_store_dir.clone(),
                    &owner,
                    &pubkey,
                    None,
                    coven_protocol::membership::MemberRole::Member,
                    &EncryptionService::from_key([42; 32]),
                    "Device Join Transport Store",
                )
                .await
                .expect("admit second joining identity");
            Some(pubkey)
        } else {
            None
        };
        let owner_device = store
            .open_into(&owner_db, owner_db_store_dir.clone())
            .await
            .expect("load membership including joiner");
        let tables = test_synced_tables();
        let snapshot_dir = tempfile::tempdir().expect("snapshot directory");
        crate::test_snapshots::publish_owner_snapshot(
            &owner_device,
            &owner_database,
            store.root(),
            snapshot_dir.path(),
        )
        .await;
        let owner_store = owner_device;
        let app = tempfile::tempdir().expect("join app directory");
        let layout = coven_foundation::store_dir::StoreLayout::new(app.path());
        let provider_binding =
            coven_storage::CloudSyncObjectStorage::provider_binding(owner_storage.as_ref())
                .await
                .expect("load owner provider binding");
        let joiner_home = match joiner_principal {
            Some(principal) => Arc::new(home.as_ref().clone().with_provider_binding(
                coven_protocol::ResolvedProviderBinding {
                    store: provider_binding.store,
                    device: coven_protocol::ProviderDeviceBinding { principal },
                },
            )),
            None => home.clone(),
        };
        let access_administrator = cross_principal.then(|| {
            coven_replication::sync::test_helpers::TestDropboxAccessAdministrator {
                namespace_id: CROSS_PRINCIPAL_NAMESPACE.to_string(),
            }
        });
        (
            Self {
                owner_store,
                owner_db,
                owner_database,
                owner_storage,
                owner_test_store: store,
                owner_store_dir: owner_db_store_dir,
                owner_keypair: owner,
                home,
                member_pubkey,
                admission,
                layout,
                tables,
                joiner_home,
                access_administrator,
                _app: app,
                _snapshot: snapshot_dir,
                _owner_store_tmp: owner_store_tmp,
            },
            second_member_pubkey,
        )
    }

    /// Capture and publish a Store snapshot covering everything the owner has
    /// materialized, then acknowledge it — the state a joining device finds
    /// when the owner's snapshot cadence has already run.
    async fn publish_owner_snapshot(&self) {
        crate::test_snapshots::publish_owner_snapshot(
            &self.owner_store,
            &self.owner_database,
            self.owner_test_store.root(),
            self._snapshot.path(),
        )
        .await;
    }

    /// Admit one more member, growing the founder's membership stream by one
    /// entry without adding an announcement stream.
    async fn admit_extra_member(&self) {
        let member = UserKeypair::generate();
        self.owner_test_store
            .admit_member(
                &self.owner_db,
                self.owner_store_dir.clone(),
                &self.owner_keypair,
                &pubkey_hex(&member),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Device Join Transport Store",
            )
            .await
            .expect("admit an extra member");
    }

    /// Admit and activate a second device that then publishes its own rows, so
    /// the store's history runs on two announcement streams rather than one.
    /// A snapshot's coverage names a tip per stream, and a bootstrap credits
    /// each stream's tip independently — a single-stream fixture cannot tell a
    /// walk that handles one stream from one that handles all of them.
    async fn publish_second_stream(&self, rows: usize) {
        let member = UserKeypair::generate();
        self.owner_test_store
            .admit_member(
                &self.owner_db,
                self.owner_store_dir.clone(),
                &self.owner_keypair,
                &pubkey_hex(&member),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Device Join Transport Store",
            )
            .await
            .expect("admit the second publishing member");
        let member_store_dir = test_store_dir();
        let member_db = open_test_db(member_store_dir.clone());
        let member_device = self
            .owner_test_store
            .activate_joined_device(
                &self.owner_db,
                self.owner_store_dir.clone(),
                &member_db,
                member_store_dir.clone(),
                &member,
                "2026-07-16T00:00:00Z",
            )
            .await
            .expect("activate the second publishing device");
        for index in 0..rows {
            member_db
                .execute_test_host_write(&format!(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                     VALUES ('second-stream-{index}', 'second {index}', 1, \
                     '{:013}-0000-member', '2026-01-01')",
                    5000 + index
                ))
                .await;
            member_device
                .run_cycle(None)
                .await
                .expect("publish the second device's Store write");
        }
        // The owner has to materialize that stream before it can cover it.
        self.owner_store
            .run_cycle(None)
            .await
            .expect("pull the second device's history onto the owner");
    }

    /// Publish one ordinary Store commit of the owner's — a row write, the kind
    /// a connected host's sync loop publishes on its own cadence — and return
    /// the commit it landed at.
    async fn publish_owner_row(
        &self,
        id: &str,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        self.owner_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('{id}', '{id}', 1, '2026-07-16T00:00:00Z', '2026-07-16T00:00:00Z')"
            ))
            .await;
        assert!(
            self.owner_test_store
                .publish_pending(&self.owner_db, &self.owner_store_dir)
                .await
                .expect("publish the owner's own Store write"),
            "the owner's row write produced no Store commit",
        );
        self.owner_store
            .latest_store_position()
            .await
            .expect("read the owner's latest Store position")
            .expect("the owner's write landed at a Store commit")
    }

    /// What the admitting device's journal says about this attempt, if it
    /// still holds a row for it at all.
    async fn owner_status(
        &self,
        bundle: &DeviceJoinOfferBundle,
    ) -> Option<coven_replication::sync::DeviceJoinStatus> {
        self.owner_database
            .device_join_status(
                bundle.offer.attempt_id,
                coven_replication::sync::DeviceJoinRole::Owner,
            )
            .await
            .expect("read the admitting device's join status")
    }

    /// Every row the joining device's pending journal still holds.
    fn pending_journal_records(
        &self,
    ) -> Vec<coven_protocol::store_commit::device_join_journal::DeviceJoinJournalRecord> {
        self.client()
            .pending_journal_records_for_test()
            .expect("read the joining device's pending journal")
    }

    /// A fresh joining client, as a relaunched app would construct it: nothing
    /// but the codes and the on-disk journal carry across.
    fn client(&self) -> crate::joining::client::DeviceJoinClient {
        crate::joining::client::DeviceJoinClient::new(
            self.admission.clone(),
            self.member_pubkey.clone(),
            self.layout.clone(),
            self.tables.clone(),
            test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            None,
            Arc::new(SystemClock),
        )
        .expect("construct DeviceJoinClient")
        .with_test_bootstrap_home(self.joiner_home.clone())
    }

    /// Begin a join and mint the offer bundle the host would encode as a QR.
    async fn begin(&self) -> DeviceJoinOfferBundle {
        self.begin_for(&self.member_pubkey).await
    }

    async fn begin_for(&self, member_pubkey: &str) -> DeviceJoinOfferBundle {
        let offer = self
            .owner_store
            .begin_device_join(member_pubkey)
            .await
            .expect("begin join");
        self.owner_store
            .device_join_transport()
            .allocate_bundle(offer)
            .await
            .expect("allocate the attempt's transport slots")
    }

    async fn drive_owner(
        &self,
        bundle: &DeviceJoinOfferBundle,
    ) -> Result<coven_replication::sync::DeviceJoinDriveOutcome, DeviceJoinTransportError> {
        self.drive_owner_with(bundle, timing()).await
    }

    /// One run of the admitting driver. A run that ends in a timeout is a
    /// process that died waiting for its counterpart; the next one resumes.
    async fn drive_owner_with(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
    ) -> Result<coven_replication::sync::DeviceJoinDriveOutcome, DeviceJoinTransportError> {
        self.drive_owner_observing(bundle, timing, &|_| {}).await
    }

    async fn drive_owner_observing(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
        on_progress: &(dyn Fn(coven_replication::sync::AdmittingDeviceJoinProgress) + Send + Sync),
    ) -> Result<coven_replication::sync::DeviceJoinDriveOutcome, DeviceJoinTransportError> {
        self.owner_store
            .device_join_transport()
            .drive(
                bundle,
                coven_replication::sync::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                self.access_administrator.as_ref().map(|administrator| {
                    administrator as &dyn coven_replication::sync::DeviceProviderAccessAdministrator
                }),
                on_progress,
                timing,
            )
            .await
    }

    /// Drop this device's owner journal row for the attempt, leaving the store
    /// in the state a device that never issued the offer is in.
    async fn forget_owner_journal(&self, bundle: &DeviceJoinOfferBundle) {
        self.owner_database
            .forget_for_test(
                bundle.offer.attempt_id,
                coven_replication::sync::store::DeviceJoinRole::Owner,
            )
            .await
            .expect("drop the owner journal row");
    }

    fn transport<'a>(&'a self, bundle: &'a DeviceJoinOfferBundle) -> DeviceJoinTransport<'a> {
        DeviceJoinTransport::open(&*self.owner_storage, bundle, DeviceJoinRole::Owner)
            .expect("open the transport")
    }

    /// Every provider key still under this attempt's transport namespace.
    ///
    /// The teardown deletes what a listing names rather than the kinds it was
    /// compiled with, so what it has to leave behind is nothing at all.
    fn attempt_namespace_keys(&self, bundle: &DeviceJoinOfferBundle) -> Vec<String> {
        let prefix = format!("{}/", bundle.transport.attempt_namespace);
        self.home
            .keys()
            .into_iter()
            .filter(|key| key.starts_with(&prefix))
            .collect()
    }

    async fn slot_bytes(
        &self,
        bundle: &DeviceJoinOfferBundle,
        kind: DeviceJoinTransportKind,
    ) -> Option<Vec<u8>> {
        self.home.read_at(slot(bundle, kind)).await.ok()
    }
}

/// The saved config of a join that ran to membership, or a panic naming what
/// the joining device got instead.
fn joined(
    outcome: Result<crate::joining::DeviceJoinTransportOutcome, crate::joining::BootstrapError>,
) -> coven_foundation::config::Config {
    match outcome.expect("the joining device finishes without error") {
        crate::joining::DeviceJoinTransportOutcome::Joined(config) => config,
        crate::joining::DeviceJoinTransportOutcome::Abandoned(_) => {
            panic!("the join was abandoned, not completed")
        }
    }
}

/// The activation of a drive that ran to membership.
fn activated(
    outcome: Result<coven_replication::sync::DeviceJoinDriveOutcome, DeviceJoinTransportError>,
) -> coven_replication::sync::DeviceJoinActivation {
    match outcome.expect("the admitting side finishes without error") {
        coven_replication::sync::DeviceJoinDriveOutcome::Activated(activation) => activation,
        coven_replication::sync::DeviceJoinDriveOutcome::Abandoned(_) => {
            panic!("the attempt was abandoned, not activated")
        }
    }
}

fn slot(bundle: &DeviceJoinOfferBundle, kind: DeviceJoinTransportKind) -> &ObjectSlot {
    bundle
        .transport
        .slots
        .get(&kind)
        .expect("every kind has a slot")
}

/// A deadline short enough that a driver with no counterpart running gives up
/// promptly. It bounds only the wait for an artifact, never the work between
/// artifacts, so a slow bootstrap still runs to completion.
fn one_shot() -> DeviceJoinTransportTiming {
    DeviceJoinTransportTiming {
        poll: Duration::from_millis(2),
        deadline: Duration::from_millis(300),
    }
}

fn assert_joiner_waited_for(
    result: Result<crate::joining::DeviceJoinTransportOutcome, crate::joining::BootstrapError>,
    kind: DeviceJoinTransportKind,
) {
    match result {
        Err(crate::joining::BootstrapError::DeviceJoinTransport(
            DeviceJoinTransportError::Timeout { kind: waited, .. },
        )) if waited == kind => {}
        other => panic!("the joiner should have died waiting for {kind:?}, got {other:?}"),
    }
}

fn assert_owner_waited_for(
    result: Result<coven_replication::sync::DeviceJoinDriveOutcome, DeviceJoinTransportError>,
    kind: DeviceJoinTransportKind,
) {
    match result {
        Err(DeviceJoinTransportError::Timeout { kind: waited, .. }) if waited == kind => {}
        other => panic!("the admitting side should have died waiting for {kind:?}, got {other:?}"),
    }
}

#[path = "transport_exchange_tests.rs"]
mod exchange_tests;
#[path = "transport_history_tests.rs"]
mod history_tests;
