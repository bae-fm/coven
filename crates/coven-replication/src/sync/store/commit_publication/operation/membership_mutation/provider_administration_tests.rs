use crate::sync::test_helpers::{
    open_test_db, pubkey_hex, test_cloud_home, test_store_dir, TestDevice, TestStore,
};
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::StoreDeviceRegistrationRef;

/// A founder Store plus one more activated device of the same identity: the
/// smallest shape in which administration has somewhere to go.
struct TwoDevices {
    store: std::sync::Arc<TestStore>,
    founder: TestDevice,
    _founder_db: coven_database::Database,
    _founder_store_dir: coven_foundation::store_dir::StoreDir,
    second: TestDevice,
    second_db: coven_database::Database,
    second_store_dir: coven_foundation::store_dir::StoreDir,
    founder_registration: StoreDeviceRegistrationRef,
    second_registration: StoreDeviceRegistrationRef,
    owner: UserKeypair,
}

async fn two_devices(label: &str) -> TwoDevices {
    let owner = UserKeypair::generate();
    let founder_store_dir = test_store_dir();
    let founder_db = open_test_db(founder_store_dir.clone());
    let home = test_cloud_home();
    let store = TestStore::create(
        &founder_db,
        founder_store_dir.clone(),
        label,
        owner.clone(),
        home,
    )
    .await
    .expect("create the administration test Store");
    let founder = store
        .bind_device_in(&founder_db, founder_store_dir.clone(), &owner)
        .await
        .expect("bind the founder device");
    let second_store_dir = test_store_dir();
    let second_db = open_test_db(second_store_dir.clone());
    let second = store
        .activate_joined_device(
            &founder_db,
            founder_store_dir.clone(),
            &second_db,
            second_store_dir.clone(),
            &owner,
            "0000000001000-0000-second-device",
        )
        .await
        .expect("activate the second device of the same identity");
    let founder_registration = founder.activated_registration_ref().await;
    let second_registration = second.activated_registration_ref().await;
    TwoDevices {
        store,
        founder,
        _founder_db: founder_db,
        _founder_store_dir: founder_store_dir,
        second,
        second_db,
        second_store_dir,
        founder_registration,
        second_registration,
        owner,
    }
}

/// Administration starts at the Store root's own administrator — the founder
/// registration the signed root descriptor binds — and one accepted transfer
/// moves it for every device that replays the chain, not just the one that
/// published it.
#[tokio::test]
async fn a_transfer_is_accepted_and_resolved_on_every_device() {
    let fixture = two_devices("administration-transfer-converges").await;
    let founder_membership = fixture
        .founder
        .membership()
        .await
        .expect("read the founder membership");
    assert_eq!(
        founder_membership.provider_administrator(),
        &fixture.founder_registration,
        "administration starts at the root's administrator",
    );

    fixture
        .founder
        .transfer_provider_administration(&fixture.second_registration)
        .await
        .expect("the administrator transfers administration");

    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.second_registration,
    );
    fixture
        .second
        .run_cycle(None)
        .await
        .expect("the second device replays the transfer");
    assert_eq!(
        fixture
            .second
            .membership()
            .await
            .expect("read the second device's membership")
            .provider_administrator(),
        &fixture.second_registration,
        "a device that replayed the chain resolves the same administrator",
    );
}

/// Only the device administration currently rests on can hand it on. A second
/// transfer from the device that just gave it away is refused before anything
/// is staged.
#[tokio::test]
async fn a_transfer_signed_by_a_non_administrator_is_refused() {
    let fixture = two_devices("administration-transfer-non-administrator").await;
    fixture
        .founder
        .transfer_provider_administration(&fixture.second_registration)
        .await
        .expect("the administrator transfers administration");

    let error = fixture
        .founder
        .transfer_provider_administration(&fixture.founder_registration)
        .await
        .expect_err("the former administrator cannot take administration back");

    assert!(
        matches!(
            error,
            crate::sync::store::MembershipOpsError::NotProviderAdministrator
        ),
        "{error:?}",
    );
    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.second_registration,
    );
}

/// A transfer names a device the Store can still hand administration to. A
/// registration that is not active at the predecessor cut is refused loudly,
/// rather than staged and left to fail somewhere later.
#[tokio::test]
async fn a_transfer_to_an_inactive_registration_is_refused() {
    let fixture = two_devices("administration-transfer-inactive-target").await;
    let stranger = UserKeypair::generate();
    let unregistered = StoreDeviceRegistrationRef {
        device_id: coven_protocol::store_commit::ObjectHash::digest(
            pubkey_hex(&stranger).as_bytes(),
        )
        .to_string()
        .parse()
        .expect("digest is a Store device id"),
        registration_hash: coven_protocol::store_commit::ObjectHash::digest(b"absent registration"),
        object: fixture.second_registration.object.clone(),
    };

    let error = fixture
        .founder
        .transfer_provider_administration(&unregistered)
        .await
        .expect_err("a transfer to an unregistered device is refused");

    assert!(
        matches!(
            error,
            crate::sync::store::MembershipOpsError::TransferTargetNotActive
        ),
        "{error:?}",
    );
    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.founder_registration,
        "a refused transfer leaves administration where it was",
    );
}

/// Excluding the device administration rests on is allowed, and it leaves the
/// Store with no device that can administer: the chain still names the excluded
/// registration, every consumer refuses loudly, and nothing silently falls back
/// to the founder or to any owner. This is the limit the founder device already
/// had, moved rather than created.
#[tokio::test]
async fn excluding_the_new_administrator_leaves_no_device_able_to_administer() {
    let fixture = two_devices("administration-transfer-excluded-administrator").await;
    fixture
        .founder
        .transfer_provider_administration(&fixture.second_registration)
        .await
        .expect("the administrator transfers administration");

    let proposal = match fixture
        .founder
        .propose_device_exclusion(&fixture.second_registration)
        .await
        .expect("propose excluding the new administrator")
    {
        crate::sync::store::StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => {
            proposal
        }
        other => panic!("proposal did not activate: {other:?}"),
    };
    fixture
        .founder
        .finalize_device_exclusion(&proposal)
        .await
        .expect("exclude the new administrator");

    let membership = fixture
        .founder
        .membership()
        .await
        .expect("re-read the founder membership");
    assert_eq!(
        membership.provider_administrator(),
        &fixture.second_registration,
        "exclusion does not move administration back",
    );
    let error = fixture
        .founder
        .begin_device_join(&pubkey_hex(&fixture.owner))
        .await
        .expect_err("no device can offer a join");
    assert!(
        matches!(
            error,
            crate::sync::store::DeviceJoinError::ProviderAdministratorRequired
        ),
        "{error:?}",
    );
    let error = fixture
        .founder
        .transfer_provider_administration(&fixture.founder_registration)
        .await
        .expect_err("no device can transfer administration back");
    assert!(
        matches!(
            error,
            crate::sync::store::MembershipOpsError::NotProviderAdministrator
        ),
        "{error:?}",
    );
}

/// A device that never replayed the transfer — it installed a snapshot taken
/// after it and nothing else — resolves the same administrator as a device that
/// replayed every entry. The rollup carries the transfer entry itself, so there
/// is nothing extra for a snapshot to retain.
#[tokio::test]
async fn a_device_restored_from_a_snapshot_after_a_transfer_resolves_the_new_administrator() {
    let fixture = two_devices("administration-transfer-snapshot-restore").await;
    fixture
        .founder
        .transfer_provider_administration(&fixture.second_registration)
        .await
        .expect("the administrator transfers administration");
    fixture
        .second
        .run_cycle(None)
        .await
        .expect("the new administrator replays the transfer");

    let third_store_dir = test_store_dir();
    let third = fixture
        .store
        .activate_joined_device_from_snapshot(
            &fixture.second_db,
            fixture.second_store_dir.clone(),
            third_store_dir,
            &fixture.owner,
            "0000000002000-0000-third-device",
            crate::sync::test_helpers::test_synced_tables(),
            crate::sync::test_helpers::test_migrations(),
            1,
        )
        .await
        .expect("install a third device from a snapshot taken after the transfer");

    assert_eq!(
        third
            .membership()
            .await
            .expect("read the restored device's membership")
            .provider_administrator(),
        &fixture.second_registration,
    );
}

/// Sign, stage and publish a transfer entry naming `target` from `device`,
/// skipping the operation's own guards so the Store verifier is what decides.
/// Publication verifies its own candidate before it uploads, through the same
/// `verify_membership_control_with_retained_history` a pulling device runs, so
/// a candidate refused here is a candidate no device can ever pull.
async fn publish_crafted_transfer(
    device: &TestDevice,
    signer: &UserKeypair,
    target: &StoreDeviceRegistrationRef,
) -> crate::sync::store::StoreError {
    use crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch;

    let chain = device
        .membership_for_test()
        .await
        .expect("load the exact membership chain");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize the crafting writer");
    let stream_id = writer
        .select_membership_author_stream(&chain)
        .await
        .expect("select a membership author stream");
    let entry = chain
        .signed_provider_administration_transfer_in_stream(
            signer,
            stream_id,
            target.clone(),
            "2026-09-15T00:00:00Z".to_string(),
        )
        .expect("sign the transfer entry");
    let transition = writer
        .prepare_membership_transition(&chain, entry)
        .await
        .expect("prepare the membership transition");
    let plan = writer
        .prepare_plan()
        .await
        .expect("reserve the next author position");
    let mut candidate = writer
        .prepare_candidate(
            &plan,
            StoreOperationBatch::MergeMembershipActivation {
                entry: transition.entry.clone(),
                transition: transition.transition.clone(),
                stream_activations: Vec::new(),
            },
        )
        .await
        .expect("prepare the transfer candidate");
    let publication = writer
        .finish_store_membership_transition(transition, candidate.reference.clone())
        .await
        .expect("finish the membership transition");
    candidate
        .attach_merge_membership_proof(&publication)
        .expect("attach the membership proof");
    drop(plan);
    let remotes = candidate
        .merge_membership_activation_remote_objects()
        .expect("close the candidate's remote objects");
    writer
        .stage_membership_mutation(
            b"crafted transfer".to_vec(),
            b"crafted transfer".to_vec(),
            remotes.clone(),
            candidate.clone(),
        )
        .await
        .expect("stage the crafted candidate");
    writer
        .publish_membership_authority(&candidate, &remotes)
        .await
        .expect("upload the transfer entry object");
    writer
        .publish_prepared(Box::new(candidate), None, None)
        .await
        .expect_err("the Store verifier refuses the crafted transfer")
}

/// The device that gave administration away keeps an active Owner grant, so it
/// can still author a membership entry. The verifier is what stops it: the
/// transfer's commit author has to be the administrator its own predecessor
/// resolves, and that is no longer this device.
#[tokio::test]
async fn a_transfer_published_by_a_non_administrator_is_refused_by_the_verifier() {
    let fixture = two_devices("administration-transfer-verifier-author").await;
    fixture
        .founder
        .transfer_provider_administration(&fixture.second_registration)
        .await
        .expect("the administrator transfers administration");

    let error = publish_crafted_transfer(
        &fixture.founder,
        &fixture.owner,
        &fixture.founder_registration,
    )
    .await;

    assert!(
        format!("{error:?}")
            .contains("Merge provider-administration transfer differs from its accepted authority"),
        "{error:?}",
    );
    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.second_registration,
        "a refused transfer leaves administration where the chain put it",
    );
    fixture
        .second
        .run_cycle(None)
        .await
        .expect("the other device pulls what was published");
    assert_eq!(
        fixture
            .second
            .membership()
            .await
            .expect("read the other device's membership")
            .provider_administrator(),
        &fixture.second_registration,
        "no device accepts a transfer the verifier refused",
    );
}

/// A transfer has to land on a device the Store can still hand administration
/// to. The administrator naming a registration its own predecessor device state
/// has excluded is refused, so administration can never point at a device that
/// cannot act.
#[tokio::test]
async fn a_transfer_naming_an_excluded_target_is_refused_by_the_verifier() {
    let fixture = two_devices("administration-transfer-verifier-target").await;
    let proposal = match fixture
        .founder
        .propose_device_exclusion(&fixture.second_registration)
        .await
        .expect("propose excluding the target")
    {
        crate::sync::store::StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => {
            proposal
        }
        other => panic!("proposal did not activate: {other:?}"),
    };
    fixture
        .founder
        .finalize_device_exclusion(&proposal)
        .await
        .expect("exclude the target");

    let error = publish_crafted_transfer(
        &fixture.founder,
        &fixture.owner,
        &fixture.second_registration,
    )
    .await;

    assert!(
        format!("{error:?}")
            .contains("Merge provider-administration transfer differs from its accepted authority"),
        "{error:?}",
    );
    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.founder_registration,
    );
}

/// A transfer that names the administrator it is signed by moves nothing. It is
/// refused rather than accepted as a no-op, so an accepted transfer on the chain
/// always means administration actually changed hands.
#[tokio::test]
async fn a_transfer_naming_the_current_administrator_is_refused_by_the_verifier() {
    let fixture = two_devices("administration-transfer-verifier-noop").await;

    let error = publish_crafted_transfer(
        &fixture.founder,
        &fixture.owner,
        &fixture.founder_registration,
    )
    .await;

    assert!(
        format!("{error:?}")
            .contains("Merge provider-administration transfer differs from its accepted authority"),
        "{error:?}",
    );
    assert_eq!(
        fixture
            .founder
            .membership()
            .await
            .expect("re-read the founder membership")
            .provider_administrator(),
        &fixture.founder_registration,
    );
    fixture
        .second
        .run_cycle(None)
        .await
        .expect("the other device pulls what was published");
    assert_eq!(
        fixture
            .second
            .membership()
            .await
            .expect("read the other device's membership")
            .provider_administrator(),
        &fixture.founder_registration,
    );
}
