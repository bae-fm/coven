//! Device admission driven entirely through the storage-mediated transport.
//!
//! The four-transfer join tests hand artifacts between the two sides as
//! variables. These run the same protocol with that hand-off replaced by the
//! transport's slots, over an in-memory cloud home both sides share.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::SystemClock;
use crate::database::DbError;
use crate::encryption::EncryptionService;
use crate::join_code::encode;
use crate::keys::UserKeypair;
use crate::storage::cloud::{no_progress, BlobBody, ExactSlotStorage, ObjectSlot};
use crate::sync::hlc::Hlc;
use crate::sync::store::{
    create_snapshot, DeviceJoinAction, DeviceJoinOfferBundle, DeviceJoinRoles, DeviceJoinTransport,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportTiming,
};
use crate::sync::test_helpers::*;

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

/// Everything the two sides of one join need: the owner's open Store over the
/// shared in-memory home, and a factory for the joining device's client.
struct TransportFixture {
    owner: UserKeypair,
    owner_store: crate::sync::store::Store,
    owner_database: crate::sync::store::StoreDatabase,
    owner_storage: Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    home: Arc<crate::InMemoryCloudHome>,
    provider_admin_grant: crate::ProviderAdminGrantId,
    member_pubkey: String,
    invite_code: String,
    join_request: String,
    layout: crate::store_dir::StoreLayout,
    tables: Vec<crate::sync::session::SyncedTable>,
    /// The cloud home the joining device sees. Same principal as the owner's
    /// in the ordinary case; a different account for the cross-principal one,
    /// which is what makes the admission run its provider probe.
    joiner_home: Arc<crate::InMemoryCloudHome>,
    /// The provider-side grant a cross-principal admission needs, or `None`
    /// when both sides are the same principal and no sharing step exists.
    access_administrator: Option<crate::sync::test_helpers::TestDropboxAccessAdministrator>,
    _app: tempfile::TempDir,
    _snapshot: tempfile::TempDir,
}

/// The Dropbox namespace a cross-principal fixture's store lives in.
const CROSS_PRINCIPAL_NAMESPACE: &str = "transport-shared-namespace";

impl TransportFixture {
    /// Owner and joiner on one provider account: the admission takes the
    /// same-principal path and publishes no probe.
    async fn build(store_id: &str) -> Self {
        Self::build_with(store_id, None).await
    }

    /// Owner and joiner on separate provider accounts sharing one Dropbox
    /// namespace: the admission takes the cross-principal path, so the
    /// provider probe travels through the transport with everything else.
    async fn build_cross_principal(store_id: &str) -> Self {
        Self::build_with(
            store_id,
            Some(crate::ProviderPrincipalId::Dropbox {
                account_id: "joining-device-account".to_string(),
            }),
        )
        .await
    }

    async fn build_with(
        store_id: &str,
        joiner_principal: Option<crate::ProviderPrincipalId>,
    ) -> Self {
        crate::keys::test_keyring::install();
        let owner = UserKeypair::generate();
        let owner_db = open_test_db();
        let owner_database = crate::sync::store::StoreDatabase::from_database(owner_db.clone());
        let create_store_db = owner_db.clone();
        let create_store_owner = owner.clone();
        let store_id_owned = store_id.to_string();
        let cross_principal = joiner_principal.is_some();
        let store = tokio::spawn(async move {
            if cross_principal {
                TestStore::create_with_provider_binding(
                    &create_store_db,
                    &store_id_owned,
                    create_store_owner,
                    crate::ResolvedProviderBinding {
                        store: crate::StoreProviderBinding::Dropbox {
                            namespace_id: CROSS_PRINCIPAL_NAMESPACE.to_string(),
                        },
                        device: crate::ProviderDeviceBinding {
                            principal: crate::ProviderPrincipalId::Dropbox {
                                account_id: "owner-device-account".to_string(),
                            },
                        },
                    },
                )
                .await
            } else {
                TestStore::create(&create_store_db, &store_id_owned, create_store_owner).await
            }
        })
        .await
        .expect("Store creation task")
        .expect("create Owner Store");
        let join_request = crate::generate_join_request(None).expect("generate join request");
        let member_pubkey = crate::join_code::decode_join_request(&join_request)
            .expect("decode join request")
            .public_key;
        let invitation_home = GrantingCloudHome(store.home.as_ref().clone());
        let invite = crate::sync::test_helpers::invite_store_member_for_test(
            &store.storage,
            &invitation_home,
            &owner,
            &Hlc::new("owner-device".to_string()),
            &member_pubkey,
            None,
            crate::sync::membership::MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            store_id,
            "Device Join Transport Store",
            &owner_database,
        )
        .await
        .expect("invite joiner identity");
        let membership = store
            .open_into(&owner_db)
            .await
            .expect("load membership including joiner");
        let tables = test_synced_tables();
        let snapshot_dir = tempfile::tempdir().expect("snapshot directory");
        let snapshot_path = snapshot_dir.path().to_path_buf();
        let snapshot_tables = tables.clone();
        let snapshot = owner_db
            .call(move |connection| {
                create_snapshot(connection, &snapshot_path, &snapshot_tables, None)
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .await
            .expect("create join snapshot");
        let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            snapshot,
            snapshot_coverage.clone(),
            &owner,
            &membership,
            &owner_db,
        )
        .await
        .expect("publish join snapshot");
        publish_store_ack_fixture(&owner_db, &store.storage, snapshot_coverage, &owner)
            .await
            .expect("publish join snapshot acknowledgement");
        let owner_storage = Arc::new(
            crate::sync::cloud_storage::CloudSyncStorage::new(
                store.home.clone(),
                crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key(
                    [42; 32],
                )),
                crate::sync::cloud_storage::BlobPathScheme::Hashed,
                store_id,
                owner.clone(),
            )
            .expect("construct production Store storage"),
        );
        let owner_store =
            crate::sync::store::Store::load(owner_database.clone(), owner_storage.clone())
                .await
                .expect("open production Store owner");
        let app = tempfile::tempdir().expect("join app directory");
        let layout = crate::store_dir::StoreLayout::new(app.path());
        let provider_admin_grant = store
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        let joiner_home = match joiner_principal {
            Some(principal) => Arc::new(store.home.as_ref().clone().with_provider_binding(
                crate::ResolvedProviderBinding {
                    store: store.protocol_root.descriptor.provider.clone(),
                    device: crate::ProviderDeviceBinding { principal },
                },
            )),
            None => store.home.clone(),
        };
        let access_administrator =
            cross_principal.then(
                || crate::sync::test_helpers::TestDropboxAccessAdministrator {
                    namespace_id: CROSS_PRINCIPAL_NAMESPACE.to_string(),
                },
            );
        Self {
            owner,
            owner_store,
            owner_database,
            owner_storage,
            home: store.home.clone(),
            provider_admin_grant,
            member_pubkey,
            invite_code: encode(&invite),
            join_request,
            layout,
            tables,
            joiner_home,
            access_administrator,
            _app: app,
            _snapshot: snapshot_dir,
        }
    }

    /// A fresh joining client, as a relaunched app would construct it: nothing
    /// but the codes and the on-disk journal carry across.
    fn client(&self) -> crate::DeviceJoinClient {
        crate::DeviceJoinClient::new(
            &self.invite_code,
            &self.join_request,
            self.layout.clone(),
            self.tables.clone(),
            test_migrations(),
            None,
            crate::custody::KeyCustody::Keyring,
            crate::identity_custody::IdentityCustody::Keyring,
            None,
            None,
            Arc::new(SystemClock),
        )
        .expect("construct DeviceJoinClient")
        .with_test_bootstrap_home(self.joiner_home.clone())
    }

    /// Begin a join and mint the offer bundle the host would encode as a QR.
    async fn begin(&self) -> DeviceJoinOfferBundle {
        let offer = self
            .owner_store
            .begin_device_join(
                &self.owner,
                &self.member_pubkey,
                self.provider_admin_grant.clone(),
            )
            .await
            .expect("begin join");
        DeviceJoinOfferBundle::allocate(&*self.owner_storage, offer)
            .await
            .expect("allocate the attempt's transport slots")
    }

    async fn drive_owner(
        &self,
        bundle: &DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinActivation, DeviceJoinTransportError> {
        self.drive_owner_with(bundle, timing()).await
    }

    /// One run of the admitting driver. A run that ends in a timeout is a
    /// process that died waiting for its counterpart; the next one resumes.
    async fn drive_owner_with(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinActivation, DeviceJoinTransportError> {
        crate::drive_device_join(
            &self.owner_store,
            &self.owner,
            bundle,
            crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
            self.access_administrator.as_ref().map(|administrator| {
                administrator as &dyn crate::DeviceProviderAccessAdministrator
            }),
            timing,
        )
        .await
    }

    /// The attempt reference the owner cancels, taken from its own journal.
    async fn attempt_ref(&self, bundle: &DeviceJoinOfferBundle) -> crate::DeviceJoinAttemptRef {
        match self
            .owner_database
            .device_join_status(bundle.offer.attempt_id, crate::DeviceJoinRole::Owner)
            .await
            .expect("load the owner journal")
        {
            Some(crate::DeviceJoinStatus::AwaitingChallengePublication { bootstrap }) => {
                bootstrap.publication_authorization.attempt.clone()
            }
            other => panic!("the owner has not activated the attempt yet: {other:?}"),
        }
    }

    fn transport<'a>(&'a self, bundle: &'a DeviceJoinOfferBundle) -> DeviceJoinTransport<'a> {
        DeviceJoinTransport::open(
            &*self.owner_storage,
            bundle,
            DeviceJoinRoles::admitting(true, true),
        )
        .expect("open the transport")
    }

    async fn slot_bytes(
        &self,
        bundle: &DeviceJoinOfferBundle,
        kind: DeviceJoinTransportKind,
    ) -> Option<Vec<u8>> {
        self.home.read_at(slot(bundle, kind)).await.ok()
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
#[tokio::test]
async fn transport_carries_a_whole_join_between_two_drivers() {
    tokio::spawn(run_transport_carries_a_whole_join_between_two_drivers())
        .await
        .expect("device join transport task");
}

async fn run_transport_carries_a_whole_join_between_two_drivers() {
    let fixture = TransportFixture::build("device-join-transport-happy-path").await;
    let bundle = fixture.begin().await;

    let joiner = fixture.client();
    let cancel = never_cancelled();
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), |_status| {}, &cancel)),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    let config = config.expect("joining device completes through the transport");
    let activation = activation.expect("the owner driver reaches activation");

    assert!(config.store_dir.config_path().exists());
    assert_eq!(
        activation.outcome.attempt().attempt_id,
        bundle.offer.attempt_id,
    );
    assert!(fixture
        .client()
        .resume_device_joins()
        .expect("enumerate completed joins")
        .is_empty());
    // The joiner's completion is the point every artifact has been consumed,
    // so the attempt's namespace is empty again.
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
}

/// The bundle is what the host encodes as its join code, so it has to survive
/// the trip out and back: the joining device rebuilds it from bytes alone and
/// reads the same slots and seal key the owner allocated.
#[tokio::test]
async fn the_offer_bundle_round_trips_through_its_encoded_form() {
    tokio::spawn(run_the_offer_bundle_round_trips_through_its_encoded_form())
        .await
        .expect("offer bundle task");
}

async fn run_the_offer_bundle_round_trips_through_its_encoded_form() {
    let fixture = TransportFixture::build("device-join-transport-bundle").await;
    let bundle = fixture.begin().await;

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
    DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRoles::joiner())
        .expect("open transport")
        .publish(&DeviceJoinAction::TransferProviderAccessRequest(
            request.clone(),
        ))
        .await
        .expect("publish the access request");
    let read_back = DeviceJoinTransport::open(&joiner_storage, &decoded, DeviceJoinRoles::joiner())
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
}

/// The same admission when the joining device is on a different provider
/// account than the owner: the protocol adds its cross-principal probe, and the
/// transport carries the larger artifacts without knowing they grew.
#[tokio::test]
async fn transport_carries_a_cross_principal_join() {
    tokio::spawn(run_transport_carries_a_cross_principal_join())
        .await
        .expect("cross-principal join task");
}

async fn run_transport_carries_a_cross_principal_join() {
    let fixture = TransportFixture::build_cross_principal("device-join-transport-cross").await;
    let bundle = fixture.begin().await;

    let joiner = fixture.client();
    let cancel = never_cancelled();

    // Advance far enough to read the approval off its slot, so the test proves
    // this really took the probe path rather than the same-principal one.
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), |_status| {}, &cancel)).await,
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
                crate::sync::store::DeviceProviderAdmissionChallenge::CrossPrincipal(_)
            ),
            "separate provider accounts must admit through the cross-principal probe",
        ),
        other => panic!("the approval slot holds the approval, got {other:?}"),
    }

    let finishing_joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(finishing_joiner.join_via_transport(&bundle, timing(), |_status| {}, &cancel)),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    let config = config.expect("the joining device completes across provider accounts");
    let activation = activation.expect("the owner driver reaches activation");

    assert!(config.store_dir.config_path().exists());
    assert_eq!(
        activation.outcome.attempt().attempt_id,
        bundle.offer.attempt_id,
    );
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

/// Run the join one side at a time, each run a process that dies waiting for
/// the counterpart that is not running. Every restart resumes from the durable
/// journal and the slots, republishing what it already published — byte for
/// byte — and reading back what it already consumed.
#[tokio::test]
async fn each_side_resumes_from_every_artifact_boundary() {
    tokio::spawn(run_each_side_resumes_from_every_artifact_boundary())
        .await
        .expect("crash and resume task");
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
                .join_via_transport(bundle, timing, |_status| {}, cancel)
                .await
        }
    };

    // The joiner publishes its access request, then dies waiting for an
    // administrator that is not running.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::ProviderAdmissionApproval,
    );
    let access_request = fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
        .await
        .expect("the access request survived the joiner's death");

    // The administrator approves and dies waiting for the registration request.
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::RegistrationRequest,
    );
    let approval = fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAdmissionApproval)
        .await
        .expect("the approval survived the driver's death");

    // A relaunched joiner republishes the identical access request, consumes
    // the approval, and dies waiting for the provider-ready bootstrap.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::ProviderReadyBootstrap,
    );
    assert_eq!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .as_ref(),
        Some(&access_request),
        "a resumed joiner left its first access request's exact bytes in place",
    );

    // The administrator resumes, republishes the identical approval, publishes
    // the provider-ready bootstrap, and dies waiting for readiness.
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::Readiness,
    );
    assert_eq!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAdmissionApproval)
            .await
            .as_ref(),
        Some(&approval),
        "a resumed driver left its first approval's exact bytes in place",
    );

    // The joiner bootstraps its store, publishes readiness, and dies waiting
    // for the activation.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::Activation,
    );

    // With every joiner artifact present, the admitting side runs to activation
    // without waiting for anything.
    let activation = fixture
        .drive_owner_with(&bundle, one_shot())
        .await
        .expect("the admitting side finishes once the joiner's artifacts are all published");
    assert_eq!(
        activation.outcome.attempt().attempt_id,
        bundle.offer.attempt_id
    );

    // The joiner's last restart consumes the activation and saves the store.
    let config = Box::pin(join_once(timing()))
        .await
        .expect("the joiner completes from its final restart");
    assert!(config.store_dir.config_path().exists());
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
}

fn assert_joiner_waited_for(
    result: Result<crate::Config, crate::BootstrapError>,
    kind: DeviceJoinTransportKind,
) {
    match result {
        Err(crate::BootstrapError::DeviceJoinTransport(DeviceJoinTransportError::Timeout {
            kind: waited,
            ..
        })) if waited == kind => {}
        other => panic!("the joiner should have died waiting for {kind:?}, got {other:?}"),
    }
}

fn assert_owner_waited_for(
    result: Result<crate::DeviceJoinActivation, DeviceJoinTransportError>,
    kind: DeviceJoinTransportKind,
) {
    match result {
        Err(DeviceJoinTransportError::Timeout { kind: waited, .. }) if waited == kind => {}
        other => panic!("the admitting side should have died waiting for {kind:?}, got {other:?}"),
    }
}

/// A cancelled attempt unwinds through the same slots it advanced through, and
/// the joiner's last step — the one that consumes the cleanup activation —
/// removes the attempt's namespace. Nothing sweeps behind it.
#[tokio::test]
async fn cancelling_mid_join_removes_the_attempts_slots() {
    // Boxed rather than spawned: the cleanup-receipt preparation's future is
    // not `Send`, so it cannot cross a task boundary. The box keeps the
    // generator off the test thread's stack all the same.
    Box::pin(run_cancelling_mid_join_removes_the_attempts_slots()).await;
}

async fn run_cancelling_mid_join_removes_the_attempts_slots() {
    let fixture = TransportFixture::build("device-join-transport-cancel").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    // Advance far enough that both sides hold real state: the joiner has
    // prepared its registration request, and the owner has activated the
    // attempt it is about to cancel.
    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), |_status| {}, &cancel)).await,
        DeviceJoinTransportKind::ProviderAdmissionApproval,
    );
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::RegistrationRequest,
    );
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), |_status| {}, &cancel)).await,
        DeviceJoinTransportKind::ProviderReadyBootstrap,
    );
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::Readiness,
    );
    assert!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::RegistrationRequest)
            .await
            .is_some(),
        "the attempt reached the transport before it was cancelled",
    );

    let attempt = fixture.attempt_ref(&bundle).await;
    let closing_joiner = fixture.client();
    let (owner_unwind, joiner_unwind) = tokio::join!(
        Box::pin(crate::cancel_device_join_via_transport(
            &fixture.owner_store,
            &fixture.owner,
            &bundle,
            attempt,
            timing(),
        )),
        Box::pin(closing_joiner.close_device_join_via_transport(&bundle, timing())),
    );
    owner_unwind.expect("the owner carries the cancellation to its activated cleanup");
    joiner_unwind.expect("the joining device closes and discards its pending join");

    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the cancelled attempt",
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
    tokio::spawn(run_republishing_is_idempotent_and_a_different_artifact_is_refused())
        .await
        .expect("duplicate publish task");
}

async fn run_republishing_is_idempotent_and_a_different_artifact_is_refused() {
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
    let transport = DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRoles::joiner())
        .expect("open transport");

    let action = DeviceJoinAction::TransferProviderAccessRequest(request);
    transport.publish(&action).await.expect("first publish");
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

    // A request against a second attempt is a different artifact of the same
    // kind, and the occupied slot refuses it.
    let second = fixture.begin().await;
    let other_request = fixture
        .client()
        .prepare_provider_access_request(second.offer.clone())
        .await
        .expect("prepare a different access request");
    let conflict = transport
        .publish(&DeviceJoinAction::TransferProviderAccessRequest(
            other_request,
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
}

/// Each artifact kind has one producing role, and a transport opened for other
/// roles will not write it — the slot a counterpart reads only ever holds bytes
/// the role that owns that step put there.
#[tokio::test]
async fn a_role_cannot_publish_another_roles_artifact() {
    tokio::spawn(run_a_role_cannot_publish_another_roles_artifact())
        .await
        .expect("producer role task");
}

async fn run_a_role_cannot_publish_another_roles_artifact() {
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
                role: crate::DeviceJoinRole::Joiner,
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
}

/// With no counterpart running, awaiting an artifact fails at its deadline and
/// names the role that never published — what a host renders as "the owner's
/// app must be open".
#[tokio::test]
async fn awaiting_an_absent_counterpart_times_out_naming_its_role() {
    tokio::spawn(run_awaiting_an_absent_counterpart_times_out_naming_its_role())
        .await
        .expect("timeout task");
}

async fn run_awaiting_an_absent_counterpart_times_out_naming_its_role() {
    let fixture = TransportFixture::build("device-join-transport-timeout").await;
    let bundle = fixture.begin().await;
    let transport = fixture.transport(&bundle);

    let expired = DeviceJoinTransportTiming {
        poll: Duration::from_millis(1),
        deadline: Duration::from_millis(20),
    };
    let timed_out = transport
        .await_artifact::<crate::DeviceProviderAccessRequest>(expired)
        .await;
    assert!(
        matches!(
            timed_out,
            Err(DeviceJoinTransportError::Timeout {
                kind: DeviceJoinTransportKind::ProviderAccessRequest,
                producer: crate::DeviceJoinRole::Joiner,
            })
        ),
        "an absent joiner surfaces as a timeout naming it, got {timed_out:?}",
    );
}

/// Bytes swapped in the slot behind the transport's back do not advance the
/// join: the seal refuses them, and the awaiting driver surfaces that rather
/// than feeding anything to the protocol.
#[tokio::test]
async fn tampered_slot_bytes_refuse_to_open() {
    tokio::spawn(run_tampered_slot_bytes_refuse_to_open())
        .await
        .expect("sabotage task");
}

async fn run_tampered_slot_bytes_refuse_to_open() {
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
    DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRoles::joiner())
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
    fixture
        .home
        .create_at(target, BlobBody::from_bytes(sealed), &no_progress())
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
}

/// Two attempts against one store never touch each other's slots: each is
/// namespaced by its own attempt id, and each carries its own seal key.
#[tokio::test]
async fn concurrent_attempts_keep_separate_namespaces() {
    tokio::spawn(run_concurrent_attempts_keep_separate_namespaces())
        .await
        .expect("concurrent attempts task");
}

async fn run_concurrent_attempts_keep_separate_namespaces() {
    let fixture = TransportFixture::build("device-join-transport-concurrent").await;
    let first = fixture.begin().await;
    let second = fixture.begin().await;

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
    DeviceJoinTransport::open(&joiner_storage, &first, DeviceJoinRoles::joiner())
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
}

struct GrantingCloudHome(crate::InMemoryCloudHome);

#[async_trait::async_trait]
impl crate::storage::cloud::CloudHome for GrantingCloudHome {
    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), crate::storage::cloud::CloudHomeError> {
        self.0.put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, crate::storage::cloud::CloudHomeError> {
        self.0.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.0.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, crate::storage::cloud::CloudHomeError> {
        self.0.read(key).await
    }

    async fn read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, crate::storage::cloud::CloudHomeError> {
        self.0.read_range(key, start, end).await
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::storage::cloud::CloudHomeError> {
        self.0.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), crate::storage::cloud::CloudHomeError> {
        self.0.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, crate::storage::cloud::CloudHomeError> {
        self.0.exists(key).await
    }

    async fn set_access(
        &self,
        desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, crate::storage::cloud::CloudHomeError>
    {
        match desired {
            crate::storage::cloud::CloudAccessState::Present { .. } => {
                Ok(crate::storage::cloud::CloudAccessOutcome::Present(
                    crate::storage::cloud::CloudHomeJoinInfo::S3 {
                        bucket: "test-bucket".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        access_key: "test-access-key".to_string(),
                        secret_key: "test-secret-key".to_string(),
                        key_prefix: None,
                    },
                ))
            }
            absent => self.0.set_access(absent).await,
        }
    }
}
