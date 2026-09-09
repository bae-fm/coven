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
    assert!(approval.access_grant().is_none());
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
async fn provider_access_grant_create_resumes_after_pre_visibility_failure_on_merge() {
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
    exercise_provider_access_grant_create_interruption(
        &coven_database::StoreDatabase::new(&owner_db),
        &owner_db_store_dir,
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
    exercise_provider_access_grant_create_interruption(
        &coven_database::StoreDatabase::new(&owner_db),
        &owner_db_store_dir,
        &storage,
        &owner,
        &member,
        ExactCreateInterruption::AfterVisibility,
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
