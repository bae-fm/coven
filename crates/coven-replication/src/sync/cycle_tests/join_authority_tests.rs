use super::*;

/// A Join that already holds the author's turn publishes before a queued host write.
#[tokio::test(start_paused = true)]
async fn same_principal_activation_holds_its_position_against_the_owners_own_sync_loop() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member,
    } = owner_and_member().await;
    let mut approval = SamePrincipalApprovalFixture::prepare(
        &owner_db,
        owner_db_store_dir.clone(),
        &storage,
        &owner,
        &member,
    )
    .await;
    let request = approval
        .pending_join
        .prepare_registration_request(approval.approval)
        .await
        .expect("prepare the joining device's registration request");
    approval
        .owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish the direct join snapshot");

    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('queued', 'queued by the owner', NULL, 1, \
         '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let previous = storage
        .latest_store_position()
        .await
        .expect("read predecessor before the competing operations")
        .expect("member admission has an accepted predecessor");
    let store_dir = owner_db_store_dir.clone();

    let mut test_points = owner_db.observe_test_points();
    let (position_held, resume_acceptance) =
        owner_db.arm_test_pause(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld);
    let accept_store = approval.owner.clone();
    let acceptance = tokio::spawn(async move {
        accept_store
            .activate_same_principal_join_for_test(request)
            .await
    });

    // Hold the acceptance exactly where it has read the position and not yet
    // published the head that takes it — the window the sync loop used to
    // publish into.
    position_held.notified().await;
    let drain_db = owner_db.clone();
    let drain_cloud_storage = cloud_storage;
    let drain_store_dir = store_dir;
    let drain = tokio::spawn(async move {
        let store = crate::sync::store::Store::load(
            coven_database::StoreDatabase::new(&drain_db),
            drain_cloud_storage,
            drain_store_dir,
            owner.clone(),
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
        )
        .await
        .expect("load the owner's Store");
        let mut writer = store
            .authorize_writer()
            .await
            .expect("authorize the owner's registered writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare queued write after the Join's author turn"));
        Box::pin(writer.drain_store_writes()).await
    });
    // The competing drain cannot upload while the Join owns preparation.
    let reached_the_position = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(point) = test_points.recv().await {
            if matches!(
                point,
                coven_database::DatabaseTestPoint::StoreWriteCommitUploaded { .. }
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
        .expect("the sync loop publishes after the Join");
    assert_eq!(
        accepted.activation.outcome_activation.coord.sequence(),
        previous.coord.sequence() + 1,
        "the Join keeps the author position it acquired",
    );
    assert_eq!(
        drained, 1,
        "the queued write publishes after the Join releases its author turn",
    );
}

#[tokio::test]
async fn accepted_access_survives_exclusion_but_cannot_authorize_a_new_attempt() {
    Box::pin(async {
        let founder = UserKeypair::generate();
        let founder_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let founder_db = crate::sync::test_helpers::open_test_db(founder_db_store_dir.clone());
        let storage = cycle_test_store(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            cross_principal_test_home(),
        )
        .await;
        let excluding_owner = UserKeypair::generate();
        let encryption = EncryptionService::from_key([62; 32]);
        storage
            .admit_member(
                &founder_db,
                founder_db_store_dir.clone(),
                &founder,
                &pubkey_hex(&excluding_owner),
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Test Store",
            )
            .await
            .expect("admit second exact Owner identity");
        let excluding_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let excluding_db = crate::sync::test_helpers::open_test_db(excluding_db_store_dir.clone());
        storage
            .activate_joined_device(
                &founder_db,
                founder_db_store_dir.clone(),
                &excluding_db,
                excluding_db_store_dir.clone(),
                &excluding_owner,
                "2026-07-20T00:00:00Z",
            )
            .await
            .expect("activate second Owner device");
        storage
            .promote_active_member_fixture(
                &founder_db,
                founder_db_store_dir.clone(),
                &excluding_db,
                excluding_db_store_dir.clone(),
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
            .bind_device_in(
                &excluding_db,
                excluding_db_store_dir.clone(),
                &excluding_owner,
            )
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
        admit_test_member(
            &storage,
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &joining_member,
            &EncryptionService::from_key([62; 32]),
        )
        .await;
        let pending_dir = tempfile::tempdir().expect("create join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending.sqlite"),
        )
        .expect("open join journal");
        let peer = storage
            .cross_principal_device_for_test(&joining_member, "joining-account")
            .await
            .expect("bind joining provider principal");
        let (mut pending_join, admitting_owner, approval) = prepare_cross_principal_approval(
            &founder_db,
            founder_db_store_dir.clone(),
            &storage,
            &founder,
            &joining_member,
            &pending,
            &peer,
        )
        .await;

        let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&excluding_db)
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

        let request = pending_join
            .prepare_registration_request(approval)
            .await
            .expect("exclusion preserves provider access accepted before its finalization");
        let before = storage
            .latest_store_position()
            .await
            .expect("read accepted position before rejected activation");
        admitting_owner
            .accept_device_registration_request(request)
            .await
            .expect_err("the excluded Owner cannot activate a new Attempt");
        assert_eq!(
            storage
                .latest_store_position()
                .await
                .expect("read unchanged accepted position"),
            before,
            "rejected activation leaves the accepted Store stream unchanged"
        );
    })
    .await;
}

#[tokio::test]
async fn same_principal_join_preserves_an_existing_write_reservation() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member,
    } = owner_and_member().await;
    let mut approval = SamePrincipalApprovalFixture::prepare(
        &owner_db,
        owner_db_store_dir.clone(),
        &storage,
        &owner,
        &member,
    )
    .await;
    let request = approval
        .pending_join
        .prepare_registration_request(approval.approval)
        .await
        .expect("prepare registration request");
    approval
        .owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish Join snapshot");
    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('reserved', 'Reserved write', NULL, 1,
                 '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let store = crate::sync::store::Store::load(
        StoreDatabase::new(&owner_db),
        cloud_storage,
        owner_db_store_dir.clone(),
        owner,
        Some(EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("load author");
    let mut writer = store.authorize_writer().await.expect("authorize writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("reserve host write"));
    let database = StoreDatabase::new(&owner_db);
    let reserved = database
        .oldest_prepared_store_write()
        .await
        .expect("read reserved write")
        .expect("write is prepared");
    let before = storage
        .latest_store_position()
        .await
        .expect("read accepted position");
    let error = approval
        .owner
        .activate_same_principal_join_for_test(request.clone())
        .await
        .expect_err("a pending Join cannot displace the reserved write");
    assert!(
        error
            .to_string()
            .contains("another local Store operation owns publication: StoreWrite"),
        "{error}"
    );
    assert_eq!(
        database
            .oldest_prepared_store_write()
            .await
            .expect("read unchanged reservation")
            .expect("reservation survives")
            .commit
            .bytes,
        reserved.commit.bytes
    );
    assert_eq!(
        storage
            .latest_store_position()
            .await
            .expect("read unchanged accepted position"),
        before
    );
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish reserved write"),
        1
    );
    let accepted = approval
        .owner
        .activate_same_principal_join_for_test(request)
        .await
        .expect("retry Join after the reserved write completes");
    assert_eq!(
        accepted.activation.outcome_activation.coord.sequence(),
        reserved.commit.value.reference().coord.sequence() + 1
    );
}
