use super::*;

#[tokio::test]
async fn same_principal_device_join_completes_on_the_runtime_stack() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = owner_and_member().await;
    let encryption = EncryptionService::from_key([43; 32]);
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &encryption,
    )
    .await;

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            T0,
        )
        .await
        .expect("activate exact joined test device");

    assert!(
        coven_database::StoreDatabase::new(&member_db)
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the public join sequence activates the joining registration",
    );
}

#[tokio::test]
async fn same_principal_admission_reuses_existing_provider_access() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([60; 32]),
    )
    .await;
    let owner_device = storage
        .bind_device_in(&owner_db, owner_db_store_dir, &owner)
        .await
        .expect("bind owner Store");
    let pending_dir = tempfile::tempdir().expect("create join directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        pending_dir.path().join("pending.sqlite"),
    )
    .expect("open join journal");
    let offer = owner_device
        .begin_device_join(&pubkey_hex(&member))
        .await
        .expect("begin exact device join");
    let pending_join = storage
        .open_pending_device_join(&pending, &member, offer)
        .await
        .expect("bind pending Store join");
    let request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("prepare exact provider request");
    let position_before = storage
        .latest_store_position()
        .await
        .expect("load Store position before provider admission");
    storage.clear_exact_creates();

    let approval = owner_device
        .authorize_device_provider_access(request, None)
        .await
        .expect("authorize same-principal provider access");

    assert!(matches!(
        approval.admission,
        coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::SamePrincipal
    ));
    assert!(
        storage.exact_creates().is_empty(),
        "same-principal approval must not publish a provider-access object",
    );
    assert_eq!(
        storage
            .latest_store_position()
            .await
            .expect("load Store position after provider admission"),
        position_before,
        "same-principal approval must not activate a provider-access Store commit",
    );
}

#[tokio::test]
async fn owner_accepts_same_principal_approval_covered_by_a_later_predecessor_head() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = owner_and_member().await;
    let mut first = SamePrincipalApprovalFixture::prepare(
        &owner_db,
        owner_db_store_dir.clone(),
        &storage,
        &owner,
        &member,
    )
    .await;
    let first_registration_request = first
        .pending_join
        .prepare_registration_request(first.approval)
        .await
        .expect("prepare first registration request");

    let second_member = UserKeypair::generate();
    let second = SamePrincipalApprovalFixture::prepare(
        &owner_db,
        owner_db_store_dir.clone(),
        &storage,
        &owner,
        &second_member,
    )
    .await;

    second
        .owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish the direct join snapshot");
    second
        .owner
        .activate_same_principal_join_for_test(first_registration_request)
        .await
        .expect("the later predecessor head preserves the first approval authority");
}

#[tokio::test]
async fn pre_attempt_device_join_abandonment_is_observed_and_retry_safe() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = owner_and_member().await;
    let encryption = EncryptionService::from_key([44; 32]);
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &encryption,
    )
    .await;
    exercise_pre_attempt_abandonment(
        &coven_database::StoreDatabase::new(&owner_db),
        &owner_db_store_dir,
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn interrupted_provider_admission_reuses_its_recorded_access_locator() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([49; 32]),
    )
    .await;
    exercise_interrupted_provider_admission(
        &coven_database::StoreDatabase::new(&owner_db),
        &owner_db_store_dir,
        &storage,
        &owner,
        &member,
    )
    .await;
}

#[tokio::test]
async fn cross_principal_challenge_create_resumes_after_pre_visibility_failure() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([50; 32]),
    )
    .await;
    exercise_challenge_create_interruption(
        &owner_db,
        &owner_db_store_dir,
        &storage,
        &owner,
        &member,
        ChallengeCreateInterruption::BeforeVisibility,
    )
    .await;
}

#[tokio::test]
async fn cross_principal_challenge_create_settles_lost_response() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([51; 32]),
    )
    .await;
    exercise_challenge_create_interruption(
        &owner_db,
        &owner_db_store_dir,
        &storage,
        &owner,
        &member,
        ChallengeCreateInterruption::AfterVisibility,
    )
    .await;
}

#[tokio::test]
async fn cross_principal_device_join_completes_on_the_runtime_stack() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    let encryption = EncryptionService::from_key([43; 32]);
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &encryption,
    )
    .await;

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_device = storage
        .install_cross_principal_device(
            member_db_store_dir.clone(),
            crate::sync::test_helpers::test_synced_tables(),
            crate::sync::test_helpers::test_migrations(),
            SCHEMA_VERSION,
            &member,
            "member-account",
            T0,
        )
        .await
        .expect("complete cross-principal device join");

    assert!(
        member_device
            .latest_local_store_device_registration()
            .await
            .expect("load joined local registration")
            .is_some_and(|registration| registration.is_activated()),
        "the cross-principal join activates the joining registration",
    );
}

/// Every data commit a Store publishes between snapshots sits past the newest
/// snapshot's coverage, so a joining device's bootstrap has to read those
/// commits' packages and materialize their rows itself. Joining a Store that
/// wrote rows after admitting the member exercises exactly that window.
#[tokio::test]
async fn cross_principal_device_join_materializes_rows_written_after_the_snapshot() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    let encryption = EncryptionService::from_key([43; 32]);
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &encryption,
    )
    .await;

    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('post-snapshot', 'Written after the snapshot', NULL, 1, \
             '0000000002000-0000-M', '2026-01-02')",
        )
        .await;
    storage
        .open_into(&owner_db, owner_db_store_dir.clone())
        .await
        .expect("open owner Store")
        .run_cycle(None)
        .await
        .expect("publish the post-snapshot row");

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_device = storage
        .install_cross_principal_device(
            member_db_store_dir.clone(),
            crate::sync::test_helpers::test_synced_tables(),
            crate::sync::test_helpers::test_migrations(),
            SCHEMA_VERSION,
            &member,
            "member-account",
            T0,
        )
        .await
        .expect("complete cross-principal device join");

    assert_eq!(
        member_device
            .query_test_text("SELECT title FROM notes WHERE id = 'post-snapshot'")
            .await,
        "Written after the snapshot",
        "the bootstrap must materialize the rows of every commit past the snapshot",
    );
}

/// An admission interrupted between the physical provider grant and the
/// approval that attests it must resume without granting again: a second
/// authority would be left behind with nothing naming it.
fn exercise_interrupted_provider_admission<'a>(
    owner_db: &'a coven_database::StoreDatabase,
    owner_db_store_dir: &'a StoreDir,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission;

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

        // The challenge reserves its slots right after the physical grant, so
        // failing the next allocation lands the interruption in the window the
        // recorded locator exists to cover.
        storage.fail_next_slot_allocations(1);
        let interrupted = peer
            .authorize_device_provider_access(&owner_device, request.clone())
            .await;
        assert!(
            interrupted.is_err(),
            "the injected challenge failure surfaces"
        );
        assert_eq!(
            peer.access_grants_issued(),
            1,
            "the interrupted admission created the provider authority"
        );
        let durable_locator = match owner_db
            .device_join_status(attempt_id, DeviceJoinRole::Owner)
            .await
            .expect("load interrupted admission status")
        {
            Some(DeviceJoinStatus::AccessGranted { locator, .. }) => locator,
            status => panic!("unexpected interrupted admission status: {status:?}"),
        };

        let approval = peer
            .authorize_device_provider_access(&owner_device, request)
            .await
            .expect("resume the interrupted admission");
        assert_eq!(
            peer.access_grants_issued(),
            1,
            "the resumed admission reuses the recorded provider authority"
        );
        let DeviceProviderAdmission::CrossPrincipal { locator, .. } = &approval.admission else {
            panic!("cross-principal admission carries its provider access locator");
        };
        assert_eq!(
            locator, &durable_locator,
            "the approval attests the exact authority the journal recorded"
        );

        let retry = peer
            .authorize_device_provider_access(&owner_device, (*approval.request).clone())
            .await
            .expect("retry completed provider access authorization");
        assert_eq!(retry, approval);
        assert_eq!(peer.access_grants_issued(), 1);
        assert!(matches!(
            owner_db
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load completed provider access status"),
            Some(DeviceJoinStatus::AwaitingRegistrationRequest { .. })
        ));
    })
}

#[derive(Clone, Copy)]
enum ChallengeCreateInterruption {
    BeforeVisibility,
    AfterVisibility,
}

/// Publishing the cross-principal challenge creates one provider object, and
/// the admission has to survive losing that create's outcome either way: a
/// failure before the bytes are visible leaves the attempt where it was, and a
/// lost response after they land settles against the stored bytes. Neither
/// creates a second challenge object, and neither grants provider access again.
fn exercise_challenge_create_interruption<'a>(
    owner_db: &'a Database,
    owner_db_store_dir: &'a StoreDir,
    storage: &'a TestStore,
    owner: &'a UserKeypair,
    member: &'a UserKeypair,
    interruption: ChallengeCreateInterruption,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        use coven_protocol::store_commit::device_join_exchange::{
            DeviceProviderAdmission, DeviceProviderChallengePublication,
        };

        use crate::sync::store::{DeviceJoinRole, DeviceJoinStatus};

        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let peer = storage
            .cross_principal_device_for_test(member, "joining-account")
            .await
            .expect("bind joining provider principal");
        let (mut pending_join, owner_device, approval) = prepare_cross_principal_approval(
            owner_db,
            owner_db_store_dir.clone(),
            storage,
            owner,
            member,
            &pending,
            &peer,
        )
        .await;
        let attempt_id = approval.request.offer.attempt_id;
        let DeviceProviderAdmission::CrossPrincipal {
            challenge: approved_challenge,
            ..
        } = &approval.admission
        else {
            panic!("cross-principal admission carries its probe challenge")
        };
        let approved_challenge = approved_challenge.clone();
        let administrator_slot = approved_challenge.administrator_object.slot.clone();

        let request = pending_join
            .prepare_registration_request(approval)
            .await
            .expect("prepare the registration request");
        let provisional = owner_device
            .accept_device_registration_request(request)
            .await
            .expect("accept the registration request");
        let database = StoreDatabase::new(owner_db);
        assert!(matches!(
            database
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load the accepted attempt status"),
            Some(DeviceJoinStatus::AwaitingChallengePublication { .. })
        ));

        // The challenge object is the next exact create either way, so call 1
        // after this reset is the one that carries it.
        storage.clear_exact_creates();
        match interruption {
            ChallengeCreateInterruption::BeforeVisibility => {
                storage.fail_exact_create_before_call(1)
            }
            ChallengeCreateInterruption::AfterVisibility => storage.fail_exact_create_after_call(1),
        }
        let first = owner_device
            .publish_device_provider_challenge(provisional.clone())
            .await;
        let ready = match interruption {
            ChallengeCreateInterruption::BeforeVisibility => {
                assert!(
                    first.is_err(),
                    "the injected create fails before the challenge is visible"
                );
                assert!(
                    matches!(
                        database
                            .device_join_status(attempt_id, DeviceJoinRole::Owner)
                            .await
                            .expect("load the interrupted challenge status"),
                        Some(DeviceJoinStatus::AwaitingChallengePublication { .. })
                    ),
                    "a challenge that never became visible leaves the attempt where it was"
                );
                owner_device
                    .publish_device_provider_challenge(provisional.clone())
                    .await
                    .expect("resume challenge publication")
            }
            ChallengeCreateInterruption::AfterVisibility => {
                first.expect("lost create response settles against the stored challenge")
            }
        };
        let DeviceProviderChallengePublication::CrossPrincipal {
            challenge: published,
        } = &ready.challenge_publication
        else {
            panic!("a cross-principal attempt publishes its probe challenge")
        };
        assert_eq!(
            published, &approved_challenge,
            "the published challenge is the one the approval signed"
        );

        // Creates are counted where they are issued, so the pre-visibility
        // variant shows the attempt that carried no bytes alongside the retry
        // that landed them. Either way the object exists once.
        let challenge_creates = |storage: &TestStore| {
            storage
                .exact_creates()
                .iter()
                .filter(|slot| *slot == &administrator_slot)
                .count()
        };
        let creates_through_publication = challenge_creates(storage);
        assert_eq!(
            creates_through_publication,
            match interruption {
                ChallengeCreateInterruption::BeforeVisibility => 2,
                ChallengeCreateInterruption::AfterVisibility => 1,
            },
            "the challenge object landed once"
        );

        let retry = owner_device
            .publish_device_provider_challenge(provisional)
            .await
            .expect("retry completed challenge publication");
        assert_eq!(retry, ready);
        assert_eq!(
            challenge_creates(storage),
            creates_through_publication,
            "a published challenge is read back, not created again"
        );
        assert_eq!(
            peer.access_grants_issued(),
            1,
            "publishing the challenge does not grant provider access again"
        );
        assert!(matches!(
            database
                .device_join_status(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("load the published challenge status"),
            Some(DeviceJoinStatus::AwaitingReadiness { .. })
        ));
    })
}

/// Admitting a join is one device's job, named by its exact registration.
/// Moving administration moves that job: the device that gave it away can no
/// longer even offer a join, and the device that received it runs the whole
/// cross-principal admission.
#[tokio::test]
async fn a_transfer_moves_who_may_admit_a_cross_principal_join() {
    use crate::sync::store::DeviceJoinError;

    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        member,
        ..
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([44; 32]),
    )
    .await;
    let second_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_db = crate::sync::test_helpers::open_test_db(second_store_dir.clone());
    let second = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &second_db,
            second_store_dir,
            &owner,
            T0,
        )
        .await
        .expect("activate the owner's second device");
    let second_registration = second.activated_registration_ref().await;
    let founder = storage
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("bind the founding device");

    founder
        .transfer_provider_administration(&second_registration)
        .await
        .expect("the administrator transfers administration");
    second
        .run_cycle(None)
        .await
        .expect("the new administrator replays the transfer");

    let error = founder
        .begin_device_join(&pubkey_hex(&member))
        .await
        .expect_err("the former administrator can no longer offer a join");
    assert!(
        matches!(error, DeviceJoinError::ProviderAdministratorRequired),
        "{error:?}",
    );

    let member_store_dir = crate::sync::test_helpers::test_store_dir();
    storage
        .install_cross_principal_device_admitted_by(
            second,
            member_store_dir,
            crate::sync::test_helpers::test_synced_tables(),
            crate::sync::test_helpers::test_migrations(),
            SCHEMA_VERSION,
            &member,
            "member-account",
            T0,
        )
        .await
        .expect("the new administrator admits the cross-principal join");
}
