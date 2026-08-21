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
use coven_storage::cloud::{no_progress, ExactSlotStorage, ExactUpload};

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

/// The whole admission runs through the transport: neither driver is handed an
/// artifact, and the joining device ends with a saved member config.
#[test]
fn transport_carries_a_whole_join_between_two_drivers() {
    on_a_deep_stack(run_transport_carries_a_whole_join_between_two_drivers);
}

async fn run_transport_carries_a_whole_join_between_two_drivers() {
    let fixture = TransportFixture::build("device-join-transport-happy-path").await;
    let bundle = fixture.begin().await;
    // An object under the attempt's namespace at a name this build does not
    // know: a future protocol's artifact, or anything else a writer with
    // provider access can put there. Tearing the namespace down means this goes
    // too, and a teardown that asks for the names it was compiled with cannot
    // see it.
    let stray = fixture.home.insert_exact_object(
        &format!(
            "{}/from-another-version.json",
            bundle.transport.attempt_namespace
        ),
        b"an artifact this build has no kind for".to_vec(),
    );
    assert!(fixture.home.get(stray.logical_key()).is_some());
    fixture.home.clear_exact_creates();

    let joiner = fixture.client();
    let cancel = never_cancelled();
    let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_progress = Arc::clone(&progress);
    let observe_progress: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |phase| observed_progress.lock().expect("progress lock").push(phase));
    let owner_progress = std::sync::Mutex::new(Vec::new());
    let observe_owner_progress = |phase| {
        owner_progress
            .lock()
            .expect("owner progress lock")
            .push(phase)
    };
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), observe_progress, &cancel,)),
        Box::pin(fixture.drive_owner_observing(&bundle, timing(), &observe_owner_progress,)),
    );
    let activation = activated(activation);
    let config = joined(config);

    let registration_prefix =
        coven_protocol::store_commit::registration_semantic_prefix(&config.device_id);
    assert_eq!(
        fixture
            .home
            .exact_creates()
            .iter()
            .filter(|slot| slot.logical_key().starts_with(&registration_prefix))
            .count(),
        1,
        "the owner publishes the joining registration once; the joiner publishes only its acknowledgement",
    );

    let transport_creates = fixture
        .home
        .exact_creates()
        .into_iter()
        .filter(|slot| slot.logical_key().contains("device-join-transport"))
        .collect::<Vec<_>>();
    // A join publishes no proof of itself. The attempt and the outcome used to
    // be signed files whose every field was checked against the commit that
    // named them — the same device signing the same facts twice.
    let written = fixture
        .home
        .exact_creates()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    for class in [
        "store-v1/device-join-attempts/",
        "store-v1/device-join-outcomes/",
    ] {
        assert!(
            written.iter().all(|key| !key.starts_with(class)),
            "the join wrote a {class} object, which nothing reads: {written:?}",
        );
    }

    // One device admits, so the exchange has no handoff to carry between an
    // owner machine and a storage-administrator machine: the attempt's
    // namespace holds exactly what crosses between the two devices.
    let created_kinds = transport_creates
        .iter()
        .map(|slot| {
            slot.logical_key()
                .rsplit('/')
                .next()
                .expect("a transport slot key ends in its kind")
                .to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        created_kinds,
        std::collections::BTreeSet::from([
            "provider-access-request.json".to_string(),
            "same-principal-join.json".to_string(),
        ]),
        "a same-provider join creates only the artifacts that cross between the two devices",
    );
    for kind in ["provider-access-request", "same-principal-join"] {
        assert_eq!(
            transport_creates
                .iter()
                .filter(|slot| { slot.logical_key().ends_with(&format!("/{kind}.json")) })
                .count(),
            1,
            "the uninterrupted join attempts one create for {kind}",
        );
    }
    {
        let progress = progress.lock().expect("progress lock");
        assert!(progress.contains(
            &coven_replication::sync::JoiningDeviceJoinProgress::RequestingProviderAccess
        ));
        assert!(progress
            .contains(&coven_replication::sync::JoiningDeviceJoinProgress::WaitingForLibrary));
        assert!(progress.iter().any(|phase| matches!(
            phase,
            coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                bytes_done: 0,
                bytes_total
            } if *bytes_total > 0
        )));
        assert!(progress.iter().any(|phase| matches!(
            phase,
            coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                bytes_done,
                bytes_total
            } if bytes_done == bytes_total && *bytes_total > 0
        )));
        assert!(!progress
            .contains(&coven_replication::sync::JoiningDeviceJoinProgress::WaitingForActivation));
        assert!(!progress.contains(&coven_replication::sync::JoiningDeviceJoinProgress::CatchingUp));
        assert!(
            progress.contains(&coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary)
        );
    }
    let owner_progress = owner_progress.into_inner().expect("owner progress lock");
    assert!(owner_progress.contains(
        &coven_replication::sync::AdmittingDeviceJoinProgress::WaitingForProviderAccessRequest
    ));
    assert!(owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::RegisteringDevice));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::GrantingProviderAccess));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::PreparingLibrary));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::ActivatingDevice));

    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id,);
    assert!(fixture
        .client()
        .resume_device_joins()
        .expect("enumerate completed joins")
        .is_empty());
    // The joiner's completion is the point every artifact has been consumed,
    // so the attempt's namespace is empty again — all of it, not only the names
    // this version of the protocol happens to know.
    assert!(
        fixture.attempt_namespace_keys(&bundle).is_empty(),
        "the completed join left objects in its attempt namespace: {:?}",
        fixture.attempt_namespace_keys(&bundle),
    );
}

/// The bundle is what the host encodes as its join code, so it has to survive
/// the trip out and back: the joining device rebuilds it from bytes alone and
/// reads the same slots and seal key the owner allocated.
#[tokio::test]
async fn the_offer_bundle_round_trips_through_its_encoded_form() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-bundle").await;
        fixture
            .home
            .delay_exact_slot_allocations(Duration::from_millis(10));
        let bundle = fixture.begin().await;
        assert!(
            fixture.home.exact_slot_allocation_max_inflight() > 1,
            "independent transport slots are allocated concurrently",
        );

        let decoded = DeviceJoinOfferBundle::from_bytes(&bundle.to_bytes())
            .expect("a bundle the owner minted decodes");
        assert_eq!(decoded.offer, bundle.offer);
        assert_eq!(
            decoded.transport.attempt_namespace,
            bundle.transport.attempt_namespace
        );
        assert_eq!(decoded.transport.slots, bundle.transport.slots);

        // The decoded seal key opens what the original sealed, which is the only
        // property the joining device needs from it.
        let joiner = fixture.client();
        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(
                request.clone(),
            ))
            .await
            .expect("publish the access request");
        let read_back =
            DeviceJoinTransport::open(&joiner_storage, &decoded, DeviceJoinRole::Joiner)
                .expect("open the decoded transport")
                .read(DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .expect("read through the decoded bundle");
        assert_eq!(
            read_back,
            Some(DeviceJoinAction::TransferProviderAccessRequest(request)),
        );

        // Bytes that are not a bundle are refused rather than half-decoded.
        assert!(DeviceJoinOfferBundle::from_bytes(b"{}").is_err());
    })
    .await
    .expect("offer bundle task");
}

#[tokio::test]
async fn the_scanned_invitation_exposes_provider_credentials_only_to_its_requesting_device() {
    let fixture = TransportFixture::build("sealed-device-invitation").await;
    let bundle = fixture.begin().await;
    let mut admission = fixture.admission.clone();
    admission.join_info = coven_storage::CloudHomeJoinInfo::S3 {
        bucket: "sealed-bucket".to_string(),
        region: "sealed-region".to_string(),
        endpoint: Some("https://sealed.example".to_string()),
        access_key: "ACCESS-KEY-MUST-STAY-SEALED".to_string(),
        secret_key: "SECRET-KEY-MUST-STAY-SEALED".to_string(),
        key_prefix: Some("sealed-prefix".to_string()),
    };
    let invite = crate::joining::DeviceJoinInvite::new(admission, bundle)
        .expect("seal the invitation for its requesting device");

    let wire = invite.to_bytes();
    let visible = String::from_utf8(wire.clone()).expect("device invitation is JSON");
    for secret in [
        "ACCESS-KEY-MUST-STAY-SEALED",
        "SECRET-KEY-MUST-STAY-SEALED",
        "sealed-bucket",
        "sealed-region",
        "sealed-prefix",
    ] {
        assert!(!visible.contains(secret), "wire exposed {secret}");
    }

    let decoded = crate::joining::DeviceJoinInvite::from_bytes(&wire)
        .expect("decode the sealed invitation wire");
    assert_eq!(
        decoded
            .open_admission(&fixture.member_pubkey)
            .expect("requesting device opens the invitation")
            .store_id,
        "sealed-device-invitation",
    );
    let other_identity =
        coven_keys::keys::mint_pending_identity().expect("mint unrelated pending identity");
    let other_pubkey = coven_keys::keys::public_key_hex(&other_identity);
    assert!(matches!(
        decoded.open_admission(&other_pubkey),
        Err(crate::joining::DeviceInviteError::RecipientMismatch)
    ));
}

/// The same admission when the joining device is on a different provider
/// account than the owner: the protocol adds its cross-principal probe, and the
/// transport carries the larger artifacts without knowing they grew.
#[test]
fn transport_carries_a_cross_principal_join() {
    on_a_deep_stack(run_transport_carries_a_cross_principal_join);
}

async fn run_transport_carries_a_cross_principal_join() {
    let fixture = TransportFixture::build_cross_principal("device-join-transport-cross").await;
    let bundle = fixture.begin().await;

    let joiner = fixture.client();
    let cancel = never_cancelled();

    // Advance far enough to read the approval off its slot, so the test proves
    // this really took the probe path rather than the same-principal one.
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::ProviderAdmissionApproval,
    );
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::RegistrationRequest,
    );
    match fixture
        .transport(&bundle)
        .read(DeviceJoinTransportKind::ProviderAdmissionApproval)
        .await
        .expect("read the approval off its slot")
    {
        Some(DeviceJoinAction::TransferProviderAdmissionApproval(approval)) => assert!(
            matches!(
                approval.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal { .. }
            ),
            "separate provider accounts must admit through the cross-principal probe",
        ),
        other => panic!("the approval slot holds the approval, got {other:?}"),
    }

    let finishing_joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(finishing_joiner.join_via_transport(
            &bundle,
            timing(),
            no_join_progress(),
            &cancel,
        )),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    let config = joined(config);
    let activation = activated(activation);

    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id,);
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
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

/// Run the same-provider join one side at a time. The joining device dies after
/// publishing its request, the owner completes admission without another
/// joining-device round trip, and a fresh joining process consumes the exact
/// durable response and activation.
#[test]
fn each_side_resumes_from_every_artifact_boundary() {
    on_a_deep_stack(run_each_side_resumes_from_every_artifact_boundary);
}

async fn run_each_side_resumes_from_every_artifact_boundary() {
    let fixture = TransportFixture::build("device-join-transport-resume").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joiner publishes its access request, then dies waiting for an owner
    // that is not running.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    let access_request = fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
        .await
        .expect("the access request survived the joiner's death");

    // The request already carries the exact registration. A same-principal
    // owner can publish both the library bootstrap and activation without
    // another response from the joining device.
    let activation = activated(fixture.drive_owner_with(&bundle, one_shot()).await);
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id);
    assert!(fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::SamePrincipalJoin)
        .await
        .is_some());
    assert_eq!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .as_ref(),
        Some(&access_request),
        "the owner left the joining device's exact request in place",
    );
    // The joiner's restart republishes its identical request, installs the
    // library, consumes the activation, and saves the store.
    let config = joined(Box::pin(join_once(timing())).await);
    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
}

/// Saving local configuration is a separate durable step after the Store
/// activation. If that filesystem write fails, a fresh process resumes from
/// the activated join journal and saves the same library without downloading
/// or installing the snapshot again.
#[test]
fn config_write_failure_after_snapshot_installation_resumes_without_another_pairing() {
    on_a_deep_stack(
        run_config_write_failure_after_snapshot_installation_resumes_without_another_pairing,
    );
}

async fn run_config_write_failure_after_snapshot_installation_resumes_without_another_pairing() {
    let fixture = TransportFixture::build("device-join-config-resume").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let first_joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(first_joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel))
            .await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    let config_path = fixture
        .layout
        .store_dir("device-join-config-resume")
        .config_path();
    let block_config_path = config_path.clone();
    let block_config_write: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |progress| {
            if matches!(
                progress,
                coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary
            ) {
                std::fs::create_dir(&block_config_path)
                    .expect("occupy the config path before its atomic write");
            }
        });
    let failed = fixture
        .client()
        .join_via_transport(&bundle, timing(), block_config_write, &cancel)
        .await;
    assert!(
        matches!(failed, Err(crate::joining::BootstrapError::Config(_))),
        "the injected config write failure must reach the caller: {failed:?}",
    );
    std::fs::remove_dir(&config_path).expect("remove the config-path blocker");

    let config = joined(
        fixture
            .client()
            .join_via_transport(&bundle, timing(), no_join_progress(), &cancel)
            .await,
    );
    assert_eq!(config.store_id, "device-join-config-resume");
    assert!(config_path.is_file());
}

/// Cancelling while the snapshot bytes are arriving stops that transfer,
/// removes its staged database, and leaves the durable pairing attempt ready
/// for a fresh process to retry from the beginning of library installation.
#[test]
fn snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries() {
    on_a_deep_stack(run_snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries);
}

async fn run_snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries() {
    let fixture = TransportFixture::build("device-join-snapshot-cancel").await;
    let bundle = fixture.begin().await;
    let initial_cancel = never_cancelled();

    assert_joiner_waited_for(
        Box::pin(fixture.client().join_via_transport(
            &bundle,
            one_shot(),
            no_join_progress(),
            &initial_cancel,
        ))
        .await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    fixture
        .joiner_home
        .stream_exact_reads_in_chunks(128, Duration::from_millis(25));
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let cancel_on_download: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |progress| {
            if matches!(
                progress,
                coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                    bytes_done,
                    ..
                } if bytes_done > 0
            ) {
                cancel_tx
                    .send(true)
                    .expect("snapshot cancellation receiver remains alive");
            }
        });
    let cancelled = tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .client()
            .join_via_transport(&bundle, timing(), cancel_on_download, &cancel_rx),
    )
    .await
    .expect("snapshot cancellation must interrupt the active storage read");
    assert!(
        matches!(cancelled, Err(crate::joining::BootstrapError::Cancelled)),
        "the caller receives cancellation, got {cancelled:?}",
    );
    let store_dir = fixture.layout.store_dir("device-join-snapshot-cancel");
    assert!(
        !store_dir.db_path().exists(),
        "a cancelled snapshot transfer must not leave a database image"
    );

    fixture
        .joiner_home
        .stream_exact_reads_in_chunks(usize::MAX, Duration::ZERO);
    let config = joined(
        fixture
            .client()
            .join_via_transport(&bundle, timing(), no_join_progress(), &never_cancelled())
            .await,
    );
    assert_eq!(config.store_id, "device-join-snapshot-cancel");
    assert!(store_dir.config_path().is_file());
}

/// The owner keeps writing after it activates the joining device but before
/// that device installs its library. Enrollment opens the exact offered
/// snapshot without waiting for unrelated newer history; the ordinary sync
/// loop owns that later commit after the library is usable.
#[test]
fn a_join_opens_before_later_owner_commits_are_synced() {
    on_a_deep_stack(run_a_join_opens_before_later_owner_commits_are_synced);
}

async fn run_a_join_opens_before_later_owner_commits_are_synced() {
    let fixture = TransportFixture::build("device-join-across-owner-commits").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes the exact registration request. The owner
    // enrolls it and publishes the library bootstrap and activation.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    let activation = activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // The owner's sync loop commits a row before the joining device resumes.
    // That row is newer than the enrollment activation and therefore is not in
    // the offered snapshot.
    let intervening = fixture.publish_owner_row("owner-writes-mid-join").await;
    assert_eq!(
        intervening.coord.stream_id,
        activation.outcome_activation.coord.stream_id,
    );
    assert!(
        intervening.coord.sequence > activation.outcome_activation.coord.sequence,
        "the owner's row must be newer than the enrollment activation",
    );

    let config = joined(Box::pin(join_once(timing())).await);
    let joined_store_dir = fixture.layout.store_dir(&config.store_id);
    assert!(joined_store_dir.config_path().exists());
    // Enrollment is not a disguised sync cycle. The later row is absent from
    // the installed snapshot and will arrive through the opened library's
    // ordinary sync loop.
    let joined_db = coven_database::DatabaseImageTest::open(&joined_store_dir.db_path())
        .expect("open the joined device's database");
    assert_eq!(
        joined_db
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE id = 'owner-writes-mid-join'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count the owner's intervening row"),
        0,
        "enrollment waited for a row outside its signed snapshot cut",
    );
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

/// An owner that gives up before the attempt exists publishes its abandonment,
/// and the joining device — sitting in its wait for the library — reads that
/// instead and converges on the same terminal, clearing the namespace behind it.
#[tokio::test]
async fn an_abandoned_attempt_reaches_the_joining_device() {
    tokio::spawn(run_an_abandoned_attempt_reaches_the_joining_device())
        .await
        .expect("abandonment task");
}

async fn run_an_abandoned_attempt_reaches_the_joining_device() {
    let fixture = TransportFixture::build("device-join-transport-abandon").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    // The joining device publishes its access request and waits.
    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    let abandonment = fixture
        .owner_store
        .device_join_transport()
        .abandon(&bundle)
        .await
        .expect("the owner abandons the attempt");
    assert!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::Abandonment)
            .await
            .is_some(),
        "the abandonment reached its slot",
    );

    // The joining device's next run finds the abandonment where it would have
    // found the library bootstrap.
    match Box::pin(fixture.client().join_via_transport(
        &bundle,
        timing(),
        no_join_progress(),
        &cancel,
    ))
    .await
    .expect("the joining device accepts the abandonment")
    {
        crate::joining::DeviceJoinTransportOutcome::Abandoned(observed) => {
            assert_eq!(observed, abandonment)
        }
        crate::joining::DeviceJoinTransportOutcome::Joined(_) => {
            panic!("an abandoned attempt must not produce a member config")
        }
    }
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the abandoned attempt",
        );
    }

    // A driver started after the fact still delivers the abandonment rather
    // than waiting for a joiner that has already gone.
    match fixture
        .drive_owner_with(&bundle, one_shot())
        .await
        .expect("the admitting driver reports the abandonment")
    {
        coven_replication::sync::DeviceJoinDriveOutcome::Abandoned(observed) => {
            assert_eq!(observed, abandonment)
        }
        coven_replication::sync::DeviceJoinDriveOutcome::Activated(_) => {
            panic!("an abandoned attempt has no activation")
        }
    }
}

/// `AutoApproveSelfIssued` admits only attempts this device issued. A device
/// with no owner journal for the attempt is a device that never made the offer,
/// and it refuses rather than admitting on a stranger's say-so.
#[tokio::test]
async fn auto_approval_refuses_an_attempt_this_device_did_not_issue() {
    tokio::spawn(run_auto_approval_refuses_an_attempt_this_device_did_not_issue())
        .await
        .expect("auto approval task");
}

async fn run_auto_approval_refuses_an_attempt_this_device_did_not_issue() {
    let fixture = TransportFixture::build("device-join-transport-not-self-issued").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    // Drop this device's owner journal for the attempt: what is left is exactly
    // what a device that never issued the offer holds.
    fixture.forget_owner_journal(&bundle).await;

    let refused = fixture.drive_owner_with(&bundle, one_shot()).await;
    assert!(
        matches!(
            refused,
            Err(DeviceJoinTransportError::DeviceJoin(
                coven_replication::sync::DeviceJoinError::OfferMismatch
            ))
        ),
        "an attempt this device did not issue must be refused, got {refused:?}",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "a refused attempt produces no {kind:?}",
        );
    }
}

/// The `Ask` policy hands the request to the host and abides by the answer: a
/// refusal stops the join before a library bootstrap or activation is published.
#[tokio::test]
async fn the_ask_policy_consults_the_host_and_a_refusal_stops_the_join() {
    tokio::spawn(run_the_ask_policy_consults_the_host_and_a_refusal_stops_the_join())
        .await
        .expect("ask policy task");
}

async fn run_the_ask_policy_consults_the_host_and_a_refusal_stops_the_join() {
    let fixture = TransportFixture::build("device-join-transport-ask").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    let asked = std::sync::atomic::AtomicUsize::new(0);
    let refuse = |request: &coven_replication::sync::DeviceProviderAccessRequest| {
        assert_eq!(request.offer.attempt_id, bundle.offer.attempt_id);
        asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        coven_replication::sync::DeviceJoinApproval::Refuse
    };
    let refused = fixture
        .owner_store
        .device_join_transport()
        .drive(
            &bundle,
            coven_replication::sync::DeviceJoinApprovalPolicy::Ask(&refuse),
            None,
            &|_| {},
            one_shot(),
        )
        .await;
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the host was asked exactly once",
    );
    assert!(
        matches!(
            refused,
            Err(DeviceJoinTransportError::DeviceJoin(
                coven_replication::sync::DeviceJoinError::OfferMismatch
            ))
        ),
        "a refused request stops the join, got {refused:?}",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "a refused request produces no {kind:?}",
        );
    }

    // The same request approved by the host produces the library bootstrap and
    // activation without another joining-device round trip.
    let approve = |_request: &coven_replication::sync::DeviceProviderAccessRequest| {
        asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        coven_replication::sync::DeviceJoinApproval::Approve
    };
    activated(
        fixture
            .owner_store
            .device_join_transport()
            .drive(
                &bundle,
                coven_replication::sync::DeviceJoinApprovalPolicy::Ask(&approve),
                None,
                &|_| {},
                one_shot(),
            )
            .await,
    );
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the host was asked again on the next run",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_some(),
            "an approved request produces {kind:?}",
        );
    }
}

/// Republishing an artifact already at its slot is the same transfer, not a
/// second one: it succeeds and leaves the first write's bytes untouched, which
/// is what a crash between the journal advance and the create resumes into.
/// A *different* artifact at that slot is refused — a counterpart may already
/// have read what is there.
#[tokio::test]
async fn republishing_is_idempotent_and_a_different_artifact_is_refused() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-duplicate").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        let transport = DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport");

        let mut conflicting_request = request.clone();
        conflicting_request
            .body_mut()
            .offer
            .body_mut()
            .member_pubkey
            .push('0');
        let action = DeviceJoinAction::TransferProviderAccessRequest(request);
        let creates_before = fixture.home.exact_create_count();
        let reads_before = fixture.home.exact_full_read_count();
        transport.publish(&action).await.expect("first publish");
        assert_eq!(
            fixture.home.exact_create_count(),
            creates_before + 1,
            "a first publish creates exactly one object",
        );
        assert_eq!(
            fixture.home.exact_full_read_count(),
            reads_before,
            "a first publish does not read an empty slot before creating it",
        );
        let first_bytes = fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .expect("the access request is stored");

        transport
            .publish(&action)
            .await
            .expect("republishing the same artifact is the same transfer");
        assert_eq!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .expect("the access request is still stored"),
            first_bytes,
            "an idempotent republish leaves the first write's exact bytes",
        );

        // A different artifact of the same kind cannot replace the first one.
        // Its signature is deliberately stale because transport conflict
        // detection compares bytes; protocol acceptance is the later verifier's
        // responsibility.
        let conflict = transport
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(
                conflicting_request,
            ))
            .await;
        assert!(
            matches!(
                conflict,
                Err(DeviceJoinTransportError::ArtifactConflict {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest
                })
            ),
            "a different artifact at an occupied slot is refused, got {conflict:?}",
        );

        // A second object in the namespace, so the teardown has two independent
        // deletions to overlap. It reads what the listing names and nothing
        // else, so what it overlaps is objects that are really there.
        fixture.home.insert_exact_object(
            &format!(
                "{}/from-another-version.json",
                bundle.transport.attempt_namespace
            ),
            b"an artifact this build has no kind for".to_vec(),
        );
        fixture
            .home
            .delay_exact_full_reads(Duration::from_millis(10));
        transport
            .delete_attempt_slots()
            .await
            .expect("delete the attempt transport");
        assert!(
            fixture.home.exact_full_read_max_inflight() > 1,
            "attempt cleanup reads the objects it found concurrently",
        );
        assert!(
            fixture
                .home
                .keys()
                .iter()
                .all(|key| !key.starts_with(&bundle.transport.attempt_namespace)),
            "the teardown removed the namespace, including what it has no kind for",
        );
    })
    .await
    .expect("duplicate publish task");
}

/// Each artifact kind has one producing role, and a transport opened for other
/// roles will not write it — the slot a counterpart reads only ever holds bytes
/// the role that owns that step put there.
#[tokio::test]
async fn a_role_cannot_publish_another_roles_artifact() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-producer").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let refused = fixture
            .transport(&bundle)
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await;
        assert!(
            matches!(
                refused,
                Err(DeviceJoinTransportError::WrongProducer {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest,
                    role: coven_replication::sync::DeviceJoinRole::Joiner,
                })
            ),
            "the admitting side must not write the joiner's artifact, got {refused:?}",
        );
        assert!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_none(),
            "a refused publish never reaches storage",
        );
    })
    .await
    .expect("producer role task");
}

/// With no counterpart running, awaiting an artifact fails at its deadline and
/// names the role that never published — what a host renders as "the owner's
/// app must be open".
#[tokio::test]
async fn awaiting_an_absent_counterpart_times_out_naming_its_role() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-timeout").await;
        let bundle = fixture.begin().await;
        let transport = fixture.transport(&bundle);

        let expired = DeviceJoinTransportTiming {
            poll: Duration::from_millis(1),
            deadline: Duration::from_millis(20),
        };
        let timed_out = transport
            .await_artifact::<coven_replication::sync::DeviceProviderAccessRequest>(expired)
            .await;
        assert!(
            matches!(
                timed_out,
                Err(DeviceJoinTransportError::Timeout {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest,
                    producer: coven_replication::sync::DeviceJoinRole::Joiner,
                })
            ),
            "an absent joiner surfaces as a timeout naming it, got {timed_out:?}",
        );
    })
    .await
    .expect("timeout task");
}

/// Bytes swapped in the slot behind the transport's back do not advance the
/// join: the seal refuses them, and the awaiting driver surfaces that rather
/// than feeding anything to the protocol.
#[tokio::test]
async fn tampered_slot_bytes_refuse_to_open() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-sabotage").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await
            .expect("publish the access request");

        let target = slot(&bundle, DeviceJoinTransportKind::ProviderAccessRequest);
        let mut sealed = fixture
            .home
            .read_at(target)
            .await
            .expect("the access request is stored");
        let last = sealed.len() - 1;
        sealed[last] ^= 0xff;
        fixture
            .home
            .delete_at(target)
            .await
            .expect("clear the slot for the tampered bytes");
        let tampered_object = coven_protocol::objects::ExactObjectRef::new(
            target.clone(),
            sealed.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(&sealed),
        );
        let tampered_upload = ExactUpload::from_bytes(&tampered_object, &sealed)
            .expect("tampered bytes match their replacement exact reference");
        fixture
            .home
            .create_at(&tampered_upload, &no_progress())
            .await
            .expect("plant the tampered bytes");

        let opened = fixture
            .transport(&bundle)
            .read(DeviceJoinTransportKind::ProviderAccessRequest)
            .await;
        assert!(
            matches!(opened, Err(DeviceJoinTransportError::Unsealable(_))),
            "tampered bytes must refuse to open, got {opened:?}",
        );

        let driven = fixture.drive_owner(&bundle).await;
        assert!(
            matches!(driven, Err(DeviceJoinTransportError::Unsealable(_))),
            "a driver never advances past bytes it could not open, got {driven:?}",
        );
        assert!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAdmissionApproval)
                .await
                .is_none(),
            "no approval was produced from unopenable bytes",
        );
    })
    .await
    .expect("sabotage task");
}

/// Two attempts against one store never touch each other's slots: each is
/// namespaced by its own attempt id, and each carries its own seal key.
#[tokio::test]
async fn concurrent_attempts_keep_separate_namespaces() {
    tokio::spawn(async {
        let (fixture, second_member_pubkey) =
            TransportFixture::build_two_joiners("device-join-transport-concurrent").await;
        let first = fixture.begin().await;
        let second = fixture.begin_for(&second_member_pubkey).await;

        assert_ne!(first.offer.attempt_id, second.offer.attempt_id);
        assert_ne!(
            first.transport.attempt_namespace,
            second.transport.attempt_namespace
        );
        for kind in DeviceJoinTransportKind::ALL {
            assert_ne!(
                slot(&first, kind).logical_key(),
                slot(&second, kind).logical_key(),
                "{kind:?} slots collide across attempts",
            );
        }

        let joiner = fixture.client();
        let request = joiner
            .prepare_provider_access_request(first.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &first, DeviceJoinRole::Joiner)
            .expect("open the first attempt's transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await
            .expect("publish into the first attempt");

        assert!(
            fixture
                .slot_bytes(&first, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_some(),
            "the first attempt holds its access request",
        );
        assert!(
            fixture
                .slot_bytes(&second, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_none(),
            "the second attempt's slot is untouched",
        );
        assert!(fixture
            .transport(&second)
            .read(DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .expect("read the second attempt's empty slot")
            .is_none(),);
    })
    .await
    .expect("concurrent attempts task");
}

/// The stream and sequence a Store package object's key names, from
/// `package_semantic_prefix`: `.../packages/{stream}/{sequence}/{hash}`.
fn store_package_coordinate(logical_key: &str) -> Option<(String, u64)> {
    let (_, tail) = logical_key.split_once("/packages/")?;
    let mut parts = tail.split('/');
    let stream = parts.next()?.to_string();
    let sequence = parts.next()?.parse::<u64>().ok()?;
    Some((stream, sequence))
}

/// A joining device installs the owner's snapshot image and then owes only the
/// history published after it. The rows behind the coverage came with the
/// image, so reading a package per covered commit buys nothing — and it is what
/// made a live join over a hundred commits spend minutes reinstalling history
/// it already held.
#[test]
fn a_join_resolves_only_the_history_its_snapshot_does_not_cover() {
    on_a_deep_stack(run_a_join_resolves_only_the_history_its_snapshot_does_not_cover);
}

async fn run_a_join_resolves_only_the_history_its_snapshot_does_not_cover() {
    let fixture = TransportFixture::build("device-join-snapshot-coverage").await;
    for index in 0..8 {
        fixture.publish_owner_row(&format!("covered-{index}")).await;
    }
    // Two announcement streams, so the coverage names a tip for each and the
    // bootstrap has to credit both.
    fixture.publish_second_stream(3).await;
    fixture.publish_owner_snapshot().await;
    let coverage = fixture
        .owner_database
        .latest_local_store_snapshot()
        .await
        .expect("read the owner's published snapshot")
        .expect("the owner published a snapshot")
        .meta
        .coverage
        .commits()
        .iter()
        .map(|(stream, reference)| (stream.to_string(), reference.coord.sequence()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert!(
        coverage.len() > 1,
        "the snapshot has to cover more than one stream: {coverage:?}",
    );
    for index in 0..3 {
        fixture
            .publish_owner_row(&format!("uncovered-{index}"))
            .await;
    }

    let bundle = fixture.begin().await;
    fixture.home.clear_exact_reads();
    let cancel = never_cancelled();
    let joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), no_join_progress(), &cancel)),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    activated(activation);
    joined(config);

    let package_reads = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter_map(|slot| store_package_coordinate(slot.logical_key()))
        .collect::<Vec<_>>();
    let (covered, uncovered): (Vec<_>, Vec<_>) = package_reads
        .iter()
        .partition(|(stream, sequence)| coverage.get(stream).is_some_and(|tip| sequence <= tip));
    assert!(
        covered.is_empty(),
        "the join read packages for commits its installed snapshot already covers: \
         {covered:?} against coverage {coverage:?}",
    );
    assert!(
        !uncovered.is_empty(),
        "the join read no package at all, so it proves nothing about coverage",
    );
}

/// What one same-provider join actually handed the joining device: the closure
/// the owner carried, and the size of the journal row it sits in.
struct CarriedJoin {
    /// The Store root the closure belongs to.
    root: coven_protocol::store_commit::StoreRootRef,
    /// The cut the attempt's activation commit names, which the joining device
    /// has to land on however little the closure carries.
    bootstrap_cut: coven_protocol::store_commit::StoreHistoryCut,
    /// The snapshot's coverage, as a tip sequence per stream.
    coverage: std::collections::BTreeMap<String, u64>,
    closure: coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapClosure,
    /// The owner's journal row for the attempt, serialized as it is stored.
    journal_bytes: usize,
}

impl CarriedJoin {
    /// The stream and sequence of every commit the closure carried.
    fn carried(&self) -> Vec<(String, u64)> {
        self.closure
            .commits
            .iter()
            .map(|commit| {
                (
                    commit.reference.coord.stream_id.to_string(),
                    commit.reference.coord.sequence(),
                )
            })
            .collect()
    }
}

/// Run one whole same-provider join over a store whose history runs `covered`
/// owner commits deep behind its snapshot and three commits past it, and report
/// what the owner carried.
async fn carried_join_over(store_id: &str, covered: usize) -> CarriedJoin {
    let fixture = TransportFixture::build(store_id).await;
    for index in 0..covered {
        fixture.publish_owner_row(&format!("covered-{index}")).await;
    }
    // Two announcement streams, so the coverage names a tip for each and the
    // trimmed walk has to start from both.
    fixture.publish_second_stream(3).await;
    fixture.publish_owner_snapshot().await;
    let coverage = fixture
        .owner_database
        .latest_local_store_snapshot()
        .await
        .expect("read the owner's published snapshot")
        .expect("the owner published a snapshot")
        .meta
        .coverage
        .commits()
        .iter()
        .map(|(stream, reference)| (stream.to_string(), reference.coord.sequence()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert!(
        coverage.len() > 1,
        "the snapshot has to cover more than one stream: {coverage:?}",
    );
    for index in 0..3 {
        fixture
            .publish_owner_row(&format!("uncovered-{index}"))
            .await;
    }

    let bundle = fixture.begin().await;
    let cancel = never_cancelled();
    let joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), no_join_progress(), &cancel)),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    activated(activation);
    joined(config);

    let record = fixture
        .owner_database
        .load_device_join(
            bundle.offer.attempt_id,
            coven_replication::sync::store::DeviceJoinRole::Owner,
        )
        .await
        .expect("read the owner's join journal")
        .expect("the owner journalled the attempt it completed");
    let journal_bytes = serde_json::to_string(&record)
        .expect("the journal row serializes as it is stored")
        .len();
    let join = match *record.progress {
        coven_protocol::store_commit::device_join_journal::DeviceJoinRoleProgress::Owner(
            coven_protocol::store_commit::device_join_journal::OwnerJoinProgress::
                SamePrincipalCompleted { join, .. },
        ) => join,
        other => panic!("the owner's journal is at {other:?}, not a completed same-provider join"),
    };
    CarriedJoin {
        root: join.installation.authority.store_root.clone(),
        // The cut the attempt named is the activation commit's own predecessor
        // cut; the closure carries that commit, so the test reads it from there
        // rather than from a separate attempt file.
        bootstrap_cut: {
            let activation = join
                .installation
                .bootstrap
                .commits
                .iter()
                .find(|commit| commit.reference == join.activation.outcome_activation)
                .expect("the closure carries the attempt's activation commit");
            let commit: coven_protocol::store_commit::StoreBatchCommit =
                serde_json::from_slice(&activation.canonical_commit)
                    .expect("the carried activation commit parses");
            commit
                .order
                .predecessor_cut()
                .expect("the activation commit names a predecessor cut")
        },
        coverage,
        closure: join.installation.bootstrap,
        journal_bytes,
    }
}

/// The owner hands the joining device a snapshot image and the commits
/// published after it. Carrying the history behind that snapshot as well makes
/// the joiner parse and signature-check every commit of the store's life only
/// to discard the result at installation, and writes the whole history into the
/// owner's journal row on every attempt — which is what put a live join over
/// two hundred commits into the minutes and megabytes.
#[test]
fn a_join_carries_only_the_history_its_snapshot_does_not_cover() {
    on_a_deep_stack(run_a_join_carries_only_the_history_its_snapshot_does_not_cover);
}

async fn run_a_join_carries_only_the_history_its_snapshot_does_not_cover() {
    let shallow = carried_join_over("device-join-carry-shallow", 2).await;
    let deep = carried_join_over("device-join-carry-deep", 14).await;

    for (name, join) in [("shallow", &shallow), ("deep", &deep)] {
        let carried = join.carried();
        let covered = carried
            .iter()
            .filter(|(stream, sequence)| {
                join.coverage.get(stream).is_some_and(|tip| sequence <= tip)
            })
            .collect::<Vec<_>>();
        assert!(
            covered.is_empty(),
            "the {name} join carried commits its snapshot already covers: {covered:?} \
             against coverage {:?}",
            join.coverage,
        );
        assert!(
            !carried.is_empty(),
            "the {name} join carried no commit at all, so it proves nothing",
        );
        // The activation commit still names the whole bootstrap cut, and the
        // joining device still checks it against that cut — the second stream's
        // tip sits behind the snapshot, so the closure does not carry it and
        // the check passes on what the installed image supplies instead. The
        // join above ran to membership, which is that check passing.
        let uncarried_tips = join
            .bootstrap_cut
            .commits()
            .values()
            .filter(|reference| {
                !carried.contains(&(
                    reference.coord.stream_id.to_string(),
                    reference.coord.sequence(),
                ))
            })
            .count();
        assert!(
            uncarried_tips > 0,
            "the {name} join carried every tip of its bootstrap cut, so the cut check \
             was never asked to reach past the closure",
        );
    }

    assert_eq!(
        shallow.carried().len(),
        deep.carried().len(),
        "twelve more commits behind the snapshot changed what the join carries: \
         {:?} against {:?}",
        shallow.carried(),
        deep.carried(),
    );
    assert!(
        deep.journal_bytes < shallow.journal_bytes + shallow.journal_bytes / 10,
        "the owner's journal row grew with history behind the snapshot: \
         {} bytes over {covered_deep} covered commits against {} over {covered_shallow}",
        deep.journal_bytes,
        shallow.journal_bytes,
        covered_deep = 14,
        covered_shallow = 2,
    );
}

/// Trimming the carry does not trim the checking. Every commit still in the
/// closure is parsed against its exact reference and signature-checked against
/// its author before the joining device will build a plan from it, so an
/// altered body, a body swapped with a sibling's, and a body lifted from
/// another Store are all refused.
#[test]
fn a_trimmed_closure_is_still_verified_commit_by_commit() {
    on_a_deep_stack(run_a_trimmed_closure_is_still_verified_commit_by_commit);
}

async fn run_a_trimmed_closure_is_still_verified_commit_by_commit() {
    let join = carried_join_over("device-join-carry-verified", 2).await;
    let foreign = carried_join_over("device-join-carry-foreign", 2).await;
    coven_database::DeviceJoinBootstrapPlan::from_closure(&join.root, join.closure.clone())
        .expect("the closure the owner carried builds a plan");
    assert!(
        join.closure.commits.len() > 1,
        "the refusals below need at least two carried commits: {:?}",
        join.carried(),
    );

    let mut altered = join.closure.clone();
    altered.commits[0].canonical_commit[0] ^= 0xff;
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(&join.root, altered).is_err(),
        "a closure with an altered commit body built a plan",
    );

    let mut swapped = join.closure.clone();
    swapped.commits[0].canonical_commit = swapped.commits[1].canonical_commit.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(&join.root, swapped).is_err(),
        "a closure whose commit body belongs to a different reference built a plan",
    );

    let mut borrowed = join.closure.clone();
    borrowed.commits[0].canonical_commit = foreign.closure.commits[0].canonical_commit.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(&join.root, borrowed).is_err(),
        "a closure carrying another Store's commit body built a plan",
    );

    let mut foreign_root = join.closure.clone();
    foreign_root.commits = foreign.closure.commits.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(&join.root, foreign_root).is_err(),
        "a closure of another Store's commits built a plan for this Store's root",
    );
}

/// A membership stream is a hash-linked list: each head names the slot of the
/// next, so a walk always starts at the founder anchor and runs to the end, and
/// no part of it can be fetched in parallel or skipped. That makes walking it
/// twice exactly twice the round-trips — which is what a joining device used to
/// do, once to open its cloud home and once to install its owner anchor, for
/// about twenty-four seconds of a two-minute live join.
///
/// The two sides run one at a time here because they share one cloud home in
/// this fixture, and the claim is about what the joining device reads.
#[test]
fn a_join_walks_the_membership_chain_once() {
    on_a_deep_stack(run_a_join_walks_the_membership_chain_once);
}

async fn run_a_join_walks_the_membership_chain_once() {
    let fixture = TransportFixture::build("device-join-membership-once").await;
    // More than one membership stream, and more than one head on each, so a
    // repeated walk shows up as a repeated read rather than as a single one.
    fixture.publish_second_stream(1).await;
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes its request and dies; the owner then admits
    // it without needing the joining device back.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // From here to the end of the join, every read is the joining device's.
    fixture.home.clear_exact_reads();
    joined(Box::pin(join_once(timing())).await);

    let head_reads = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| slot.logical_key().contains("/membership/heads/"))
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let distinct = head_reads.iter().collect::<std::collections::BTreeSet<_>>();
    assert!(
        distinct.len() > 1,
        "the joining device read at most one membership head, so this proves \
         nothing about walking the chain twice",
    );
    // One read per head, plus one: installing the owner anchor resolves the
    // founder's newest head to find the registration that signed it, and it
    // does so through the store's own verifier rather than the short-lived one
    // that opened the cloud home, so that single object is fetched again. A
    // second walk would instead cost one more read for every head there is.
    assert!(
        head_reads.len() <= distinct.len() + 1,
        "the joining device read {} membership heads over {} distinct ones, \
         which is a second walk rather than a single re-read: {head_reads:?}",
        head_reads.len(),
        distinct.len(),
    );
}

/// What a joining device spends on membership does not grow with the Store's
/// membership history.
///
/// A device join opens the Store keyring out of the membership chain, so the
/// chain is the first thing it reads and none of it can come from behind the
/// keyring. Reading it change by change made "open Store storage" most of a
/// live join: two provider round trips per membership change, back to the
/// Store's founding entry. The newest published snapshot names a rollup
/// carrying all of it, so the joining device reads that instead and walks the
/// provider only for what the snapshot does not cover.
///
/// Counted here are the operations under the membership, rollup, and
/// snapshot-metadata prefixes — everything the join spends deciding what the
/// membership is. The snapshot *image* is deliberately not among them: a
/// joining device downloads one on purpose, and it is the one large transfer
/// the round-trip budget allows.
#[test]
fn a_join_reads_the_same_membership_however_deep_the_chain() {
    on_a_deep_stack(run_a_join_reads_the_same_membership_however_deep_the_chain);
}

async fn run_a_join_reads_the_same_membership_however_deep_the_chain() {
    let shallow = join_membership_operations("join-rollup-shallow", 0).await;
    let deep = join_membership_operations("join-rollup-deep", 8).await;

    assert_eq!(
        shallow, deep,
        "a join over a chain with eight more members spent {deep} membership \
         operations against {shallow}, so it still follows the history",
    );
    // Listing the snapshot prefix, the newest snapshot's metadata, the rollup,
    // the newest covered head the rollup deliberately does not hold, the absent
    // slot that ends the stream, and that newest head once more when the store's
    // own verifier resolves the registration that signed it. Written as a number
    // rather than a bound because a budget nobody wrote down is one nobody
    // notices doubling.
    assert_eq!(
        deep, 6,
        "a join spends {deep} membership operations, not the read and a probe \
         per stream the rollup exists to make it",
    );
}

/// The membership operations one whole join issues over a Store with
/// `extra_members` admitted beyond its founder.
async fn join_membership_operations(store_id: &str, extra_members: usize) -> usize {
    let fixture = TransportFixture::build(store_id).await;
    for _ in 0..extra_members {
        fixture.admit_extra_member().await;
    }
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes its request and dies; the owner then admits
    // it without needing the joining device back, so that from here on every
    // operation counted is the joining device's own.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    fixture.home.clear_exact_reads();
    fixture.home.clear_exact_listings();
    joined(Box::pin(join_once(timing())).await);

    let counted = |key: &str| {
        key.starts_with("store-v1/membership/")
            || key.starts_with("store-v1/membership-rollups/")
            || key.starts_with("store-v1/snapshots/")
    };
    fixture
        .home
        .exact_reads()
        .iter()
        .filter(|slot| counted(slot.logical_key()))
        .count()
        + fixture
            .home
            .exact_listed_prefixes()
            .iter()
            .filter(|prefix| counted(prefix))
            .count()
}

/// What a join spends on the transport when it never waits.
///
/// A device join's transport operations divide into two kinds, and only one of
/// them is the protocol. The artifacts are fixed: each one the exchange moves
/// is read once, and a joining device that resumes republishes the artifact it
/// had already published, because the joiner journal advances when the artifact
/// is *prepared* and a crash between preparing and publishing has to be
/// recoverable — so the republish is the recovery path, and the read that
/// follows its slot collision is what confirms the slot holds the same
/// transfer. The rest is polling, and polling is time, not protocol.
///
/// This is the shape with no waiting in it at all: the joining device publishes
/// its request and dies, the owner admits it without needing the joiner back,
/// and the joining device then runs once with every artifact it asks for
/// already at its slot. Whatever it spends here it spends on every join.
///
/// Only the artifacts are counted here, and exactly: a new read in the exchange
/// is a change to the protocol and should have to be written down. What the
/// watch spends is the join's duration divided by its cadence, which no count
/// taken over a leg this short can tell apart from a watch that never backs
/// off — `the_cancellation_watch_backs_off_instead_of_polling_flat_out` settles
/// that on a clock it controls.
#[test]
fn a_join_that_never_waits_spends_a_fixed_number_of_transport_operations() {
    on_a_deep_stack(run_a_join_that_never_waits_spends_a_fixed_number_of_transport_operations);
}

async fn run_a_join_that_never_waits_spends_a_fixed_number_of_transport_operations() {
    let fixture = TransportFixture::build("device-join-no-wait-budget").await;
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();
    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // From here the owner is done, so nothing the joining device asks for is
    // still on its way and every operation it makes is its own.
    fixture.home.clear_exact_reads();
    joined(Box::pin(join_once(timing())).await);

    let artifacts = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter_map(|slot| {
            slot.logical_key()
                .strip_prefix("store-v1/device-join-transport/")
                .map(|tail| tail.to_string())
        })
        .collect::<Vec<_>>();

    // Six artifacts the exchange moves, the request this resume republishes and
    // the read that confirms its slot, the step waits' first look at the
    // abandonment slot, and the teardown's probe of every slot an attempt can
    // hold before it deletes the ones it finds — which is now one listing and a
    // read per object that is really there, rather than a probe for every name
    // the build knows.
    assert_eq!(
        artifacts.len(),
        9,
        "the joining device made {} artifact operations on a join with no waiting in it: {artifacts:?}",
        artifacts.len(),
    );
    // Every one of those is a step of the exchange. A joining device used to
    // spend the whole join watching a slot for the owner cancelling, which now
    // cannot happen: nothing here is a watch.
    assert!(
        artifacts
            .iter()
            .all(|tail| !tail.ends_with("/cancellation.json")),
        "the joining device watched for a cancellation that cannot come: {artifacts:?}",
    );
}
