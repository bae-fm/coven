use super::*;
use coven_database::Database;

/// Prepare a founder Circle operation on a member device, then revoke that
/// member's grant so publishing it blocks under lost authority.
struct RevokedOperation {
    db: Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    owner_db: Database,
    owner_db_store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<TestStore>,
    founder: UserKeypair,
    successor: UserKeypair,
    operation_id: CircleOperationId,
    author_grant_id: coven_protocol::membership::MembershipGrantId,
}

impl RevokedOperation {
    async fn prepare(name: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let founder = UserKeypair::generate();
        let store = create_test_store_in_its_own_task(
            &db,
            db_store_dir.clone(),
            name,
            &founder,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let successor = UserKeypair::generate();
        let successor_pubkey = keys::public_key_hex(&successor);
        let encryption = EncryptionService::from_key([42; 32]);
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &founder,
                &successor_pubkey,
                None,
                MemberRole::Member,
                &encryption,
                "Recovery test Store",
            )
            .await
            .expect("admit successor member");
        let successor_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let successor_db = crate::sync::test_helpers::open_test_db(successor_db_store_dir.clone());
        store
            .activate_joined_device(
                &db,
                db_store_dir.clone(),
                &successor_db,
                successor_db_store_dir.clone(),
                &successor,
                "0000000001003-0000-successor",
            )
            .await
            .expect("activate successor device");
        let exact_membership = store
            .bind_device_in(&successor_db, successor_db_store_dir.clone(), &successor)
            .await
            .expect("bind successor Store")
            .membership_for_test()
            .await
            .expect("load the operation author's exact Store grant");
        let [author_grant_id] = exact_membership
            .active_grant_ids(&successor_pubkey)
            .into_iter()
            .collect::<Vec<_>>()
            .try_into()
            .expect("operation author has one active Store grant");
        // The member device is the one that authors and publishes; drive everything
        // through its database so its local membership view governs authority.
        let prepared = store
            .bind_device_in(&successor_db, successor_db_store_dir.clone(), &successor)
            .await
            .expect("bind Circle preparation Store")
            .prepare_circle_operation("0000000001003-0000-successor", "Revoked Circle")
            .await
            .expect("prepare operation while authorized");
        let operation_id = prepared.journal.operation_id.clone();
        StoreDatabase::new(&successor_db)
            .insert_circle_operation(prepared.journal, prepared.prepared_objects)
            .await
            .expect("persist operation");

        let custody = TestCustody::default();
        custody.set_initial_key([42; 32]);
        store
            .remove_member(
                &db,
                db_store_dir.clone(),
                &founder,
                &successor_pubkey,
                &encryption,
                &custody,
            )
            .await
            .expect("remove successor grant");

        Self {
            db: successor_db,
            db_store_dir: successor_db_store_dir,
            owner_db: db,
            owner_db_store_dir: db_store_dir,
            store,
            founder,
            successor,
            operation_id,
            author_grant_id,
        }
    }

    /// Publish a Store commit the removed member can accept as the witness that
    /// its own membership was revoked — the proof a discard requires.
    async fn witness_membership_revocation(&self) {
        let changeset = self
            .owner_db
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('circle-revocation-witness', 'Circle revocation witness', NULL, \
                     '0000000001004-0000-founder', '2026-01-01')",
            ])
            .await;
        StoreDatabase::new(&self.owner_db)
            .enqueue_store_changeset_for_test(changeset)
            .await
            .expect("enqueue the membership-revocation witness");
        let owner_store = self
            .store
            .bind_device(
                &self.owner_db,
                self.owner_db_store_dir.clone(),
                &self.founder,
            )
            .await
            .expect("load the revocation witness Store");
        let mut writer = owner_store
            .authorize_writer()
            .await
            .expect("authorize the revocation witness writer");
        assert!(
            writer
                .prepare_pending_store_write()
                .await
                .expect("prepare the membership-revocation witness"),
            "membership revocation must be named by a Store commit"
        );
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish the membership-revocation witness"),
            1,
            "one accepted Store commit must witness the membership revocation"
        );

        let member_store = self
            .store
            .bind_device(&self.db, self.db_store_dir.clone(), &self.successor)
            .await
            .expect("load removed member Store");
        let pull = member_store
            .authorize_writer()
            .await
            .expect("authorize removed member Store pull")
            .pull(None)
            .await
            .expect("pull the accepted membership-revocation witness");
        assert!(
            pull.held_positions.is_empty(),
            "membership-revocation witness must materialize: {:?}",
            pull.held_positions
        );
    }
}

/// The operation-inspection surface (`Circles::operations`) reports a blocked
/// operation's full shape — id, circle, intent kind, and the typed
/// `AuthorityLost` block — and `retry` refuses an operation that is not blocked
/// with the typed `NotBlocked`.
#[tokio::test]
async fn operation_inspection_surface_reports_the_typed_block() {
    let revoked = RevokedOperation::prepare("recovery-inspection-surface").await;
    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation without failing");

    let operations = StoreDatabase::new(&revoked.db)
        .get_circle_operations()
        .await
        .expect("read the operation-inspection surface");
    let info = operations
        .iter()
        .find(|info| info.operation_id == revoked.operation_id)
        .expect("the blocked operation is inspectable");
    assert_eq!(
        info.kind,
        coven_protocol::circle::CircleOperationKind::Create
    );
    assert_eq!(
        info.state,
        CircleOperationState::Blocked {
            block: coven_protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: revoked.author_grant_id.clone(),
            },
        },
    );

    // Retrying an operation that is not blocked is refused with the typed reason
    // the public `NotBlocked` error carries.
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        db_store_dir.clone(),
        "recovery-inspection-notblocked",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let prepared = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Ready Circle")
        .await
        .expect("prepare a ready operation");
    let ready_id = prepared.journal.operation_id.clone();
    coven_database::StoreDatabase::new(&db)
        .insert_circle_operation(prepared.journal, prepared.prepared_objects)
        .await
        .expect("persist the ready operation");
    let refusal = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&ready_id)
        .await
        .expect_err("a non-blocked operation is not retriable");
    assert!(
        matches!(&refusal, CircleOperationError::NotBlocked { operation_id } if *operation_id == ready_id),
        "{refusal:?}"
    );
}

#[tokio::test]
async fn a_blocked_operation_reports_typed_authority_lost() {
    let revoked = RevokedOperation::prepare("recovery-typed-block").await;
    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation without failing");
    let blocked = StoreDatabase::new(&revoked.db)
        .circle_operation(&revoked.operation_id)
        .await
        .expect("read blocked operation")
        .expect("blocked operation remains durable");
    assert_eq!(
        blocked.state(),
        CircleOperationState::Blocked {
            block: coven_protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: revoked.author_grant_id.clone(),
            },
        },
        "the block names the author's exact grant"
    );
    // Surfaced typed through the query API.
    let operations = StoreDatabase::new(&revoked.db)
        .get_circle_operations()
        .await
        .expect("read circle operations");
    assert!(operations.iter().any(|info| matches!(
        &info.state,
        CircleOperationState::Blocked {
            block: coven_protocol::circle::CircleOperationBlock::AuthorityLost { grant_id }
        } if *grant_id == revoked.author_grant_id
    )));
}

#[tokio::test]
async fn retry_of_a_blocked_operation_republishes_its_exact_prepared_commit() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        db_store_dir.clone(),
        "recovery-retry-republish",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let prepared = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare authorized founder operation");
    let operation_id = prepared.journal.operation_id.clone();
    let circle_id = prepared.journal.circle_id();
    let expected_control = prepared.journal.operation().creation.control.coord.clone();
    let expected_commit_object = prepared.journal.operation().commit_ref().object.clone();
    let founder_pubkey = keys::public_key_hex(&founder);
    let exact_membership = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind founder Store")
        .membership_for_test()
        .await
        .expect("load the founder's exact Store grant");
    let [author_grant_id] = exact_membership
        .active_grant_ids(&founder_pubkey)
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .expect("founder has one active Store grant");
    coven_database::StoreDatabase::new(&db)
        .insert_circle_operation(prepared.journal, prepared.prepared_objects)
        .await
        .expect("persist authorized operation");

    // The operation is durably blocked (its exact retained payload preserved),
    // then retried. Retry restores the phase and re-enters the publish pipeline
    // without regenerating anything.
    coven_database::StoreDatabase::new(&db)
        .block_circle_operation(
            &operation_id,
            coven_protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: author_grant_id,
            },
        )
        .await
        .expect("block the authorized operation");
    store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&operation_id)
        .await
        .expect("retry publishes the still-authorized operation");

    assert!(coven_database::StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read retried operation")
        .is_none());
    let (activated, activation_commit_ref) = coven_database::StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&founder))
        .await
        .expect("load activated Circle authoring state");
    assert_eq!(
        activated.control.coord, expected_control,
        "retry activates the exact prepared control, nothing regenerated"
    );
    assert_eq!(
        activation_commit_ref.object, expected_commit_object,
        "retry publishes the exact prepared commit object"
    );
}

/// Discard refuses an operation with no verified nonactivation proof: an
/// unpublished founder operation whose author is still authorized and whose
/// successor slot is empty. It never assumes the unseen candidate failed to
/// activate — the journal row stays durable.
#[tokio::test]
async fn discard_without_nonactivation_proof_is_refused() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "recovery-discard-refusal").await;
    let operation_id = journal.operation_id.clone();

    let refusal = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle discard Store")
        .circles()
        .discard_circle_operation(&operation_id)
        .await
        .expect_err("discard without a nonactivation proof is refused");
    assert!(
        matches!(
            &refusal,
            CircleOperationError::DiscardRequiresNonactivation { operation_id: refused }
                if *refused == operation_id
        ),
        "{refusal:?}"
    );
    assert!(
        coven_database::StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read operation after refused discard")
            .is_some(),
        "a refused discard leaves the operation durable"
    );
}

/// An accepted Store commit names the membership transition that revokes the
/// exact grant which signed the operation, and its predecessor cut excludes the
/// operation's candidate. After the removed author pulls that public witness,
/// discard verifies it, retires the candidate graph, and clears the journal.
#[tokio::test]
async fn discard_after_membership_revocation_witness_cleans_the_operation() {
    let revoked = RevokedOperation::prepare("recovery-discard-revocation").await;
    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation");

    revoked.witness_membership_revocation().await;

    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind revoked Circle discard Store")
        .circles()
        .discard_circle_operation(&revoked.operation_id)
        .await
        .expect("the accepted membership revocation permits discard");

    assert!(
        StoreDatabase::new(&revoked.db)
            .circle_operation(&revoked.operation_id)
            .await
            .expect("read discarded operation")
            .is_none(),
        "discard clears the revoked author's journal row"
    );
}

#[tokio::test]
async fn retry_refuses_active_operations_and_reblocks_idempotently() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        db_store_dir.clone(),
        "recovery-retry-refusal",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let ready = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare an authorized operation");
    let ready_id = ready.journal.operation_id.clone();
    coven_database::StoreDatabase::new(&db)
        .insert_circle_operation(ready.journal, ready.prepared_objects)
        .await
        .expect("persist the ready operation");
    let refusal = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&ready_id)
        .await
        .expect_err("retrying an operation that is not blocked is refused");
    assert!(
        matches!(&refusal, CircleOperationError::NotBlocked { operation_id } if *operation_id == ready_id),
        "{refusal}"
    );

    // A permanently-blocked operation re-blocks on retry; retrying twice leaves it
    // durably blocked with no corruption.
    let revoked = RevokedOperation::prepare("recovery-retry-reblock").await;
    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation");
    for _ in 0..2 {
        match revoked
            .store
            .bind_device(
                &revoked.db,
                revoked.db_store_dir.clone(),
                &revoked.successor,
            )
            .await
            .expect("bind Circle test Store")
            .retry_circle_operation(&revoked.operation_id)
            .await
        {
            Err(CircleOperationError::Blocked { .. }) => {}
            other => panic!("retry of a permanently-blocked operation must re-block: {other:?}"),
        }
        assert!(matches!(
            StoreDatabase::new(&revoked.db)
                .circle_operation(&revoked.operation_id)
                .await
                .expect("read re-blocked operation")
                .expect("operation remains durable")
                .state(),
            CircleOperationState::Blocked { .. }
        ));
    }
}

#[tokio::test]
async fn activation_releases_its_payload_claims_and_keeps_a_pending_operation_intact() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, home, signer, first) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-payload-activation").await;
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store");
    device
        .publish_circle_operation(&first.operation_id)
        .await
        .expect("accept the first Circle before preparing another operation");
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .admit_member(
            &db,
            db_store_dir.clone(),
            &signer,
            &member_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Circle payload Store",
        )
        .await
        .expect("admit the member whose removal starts a close");
    let components = prepare_owner_sync_components(
        &db,
        &store,
        &home,
        &db_store_dir,
        &signer,
        "circle-payload-activation",
        circle_test_custody(),
    )
    .await;
    components
        .add_circle_member(first.circle_id(), member_pubkey.clone(), CircleRole::Member)
        .await
        .expect("add the Circle member");
    let closing = components
        .remove_circle_member(first.circle_id(), member_pubkey)
        .await
        .expect("publish a close whose responses remain pending");
    let database = StoreDatabase::new(&db);
    let pending = database.circle_operation(&closing).await.unwrap().unwrap();
    assert!(matches!(
        pending.state(),
        CircleOperationState::WaitingForCloseResponses
    ));
    assert!(
        database.active_store_publication().await.unwrap().is_none(),
        "the accepted close releases its publication reservation"
    );
    let pending_objects = stored_objects(&db, &pending).await;
    let pending_claims = database
        .circle_operation_payload_claims_for_test(&closing)
        .await
        .unwrap();
    let activating = device
        .prepare_circle_operation("0000000002000-0000-creator", "Second household")
        .await
        .expect("prepare the next reserved operation");
    coven_database::StoreDatabase::new(&db)
        .insert_circle_operation(activating.journal.clone(), activating.prepared_objects)
        .await
        .expect("journal the next reserved operation");
    let activating = activating.journal;
    let prepared_steps = activating
        .operation()
        .prepared_objects
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        stored_objects(&db, &activating).await,
        prepared_steps,
        "operation insertion stores every object the operation names"
    );

    device
        .publish_circle_operation(&activating.operation_id)
        .await
        .expect("activate the founder Circle");

    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .owed_payload_cleanup()
            .await
            .expect("read the payloads still owed a deletion"),
        Vec::new(),
        "activation discharges its own cleanup obligations"
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .circle_operation_payload_claims_for_test(&activating.operation_id)
            .await
            .expect("read the activated operation's payload claims"),
        Vec::new(),
        "activation drops the operation's own claim on every payload it prepared"
    );
    let surviving = stored_objects(&db, &activating).await;
    let mut kept_a_row = false;
    for (step, object) in &activating.operation().prepared_objects {
        let row_exists = db
            .remote_object_exists_for_test(object.clone())
            .await
            .expect("read whether the activated object kept its row");
        kept_a_row |= row_exists;
        assert_eq!(
            surviving.contains(step),
            row_exists,
            "payload for {step} survives exactly while its remote object row does"
        );
    }
    assert!(
        kept_a_row && surviving.len() < activating.operation().prepared_objects.len(),
        "activation must both keep some objects and release the rest"
    );
    assert_eq!(
        stored_objects(&db, &pending).await,
        pending_objects,
        "publishing another Circle leaves the accepted close's retained payloads intact"
    );
    assert_eq!(
        database
            .circle_operation_payload_claims_for_test(&closing)
            .await
            .unwrap(),
        pending_claims
    );
    assert_eq!(
        database.circle_operation(&closing).await.unwrap(),
        Some(pending)
    );
}

/// The completing transaction of a discard drops the operation row, and with it
/// the operation's claim on every payload it prepared. A payload goes when its
/// last claim does, so what survives is exactly what a remaining `remote_objects`
/// row still names.
#[tokio::test]
async fn discard_releases_its_payload_claims() {
    let revoked = RevokedOperation::prepare("circle-payload-discard").await;
    let journal = coven_database::StoreDatabase::new(&revoked.db)
        .circle_operation(&revoked.operation_id)
        .await
        .expect("read the operation to discard")
        .expect("the operation to discard is durable");
    let device = revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind Circle test Store");
    assert!(
        !stored_objects(&revoked.db, &journal).await.is_empty(),
        "the operation owns payloads before it is discarded"
    );
    device
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation");

    revoked.witness_membership_revocation().await;

    revoked
        .store
        .bind_device(
            &revoked.db,
            revoked.db_store_dir.clone(),
            &revoked.successor,
        )
        .await
        .expect("bind revoked Circle discard Store")
        .circles()
        .discard_circle_operation(&revoked.operation_id)
        .await
        .expect("the accepted membership revocation permits discard");

    assert_eq!(
        coven_database::StoreDatabase::new(&revoked.db)
            .owed_payload_cleanup()
            .await
            .expect("read the payloads still owed a deletion"),
        Vec::new(),
        "discard discharges its own cleanup obligations"
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&revoked.db)
            .circle_operation_payload_claims_for_test(&revoked.operation_id)
            .await
            .expect("read the discarded operation's payload claims"),
        Vec::new(),
        "discard drops the operation's own claim on every payload it prepared"
    );
    let surviving = stored_objects(&revoked.db, &journal).await;
    for (step, object) in &journal.operation().prepared_objects {
        let row_exists = revoked
            .db
            .remote_object_exists_for_test(object.clone())
            .await
            .expect("read whether the discarded object kept its row");
        assert_eq!(
            surviving.contains(step),
            row_exists,
            "payload for {step} survives exactly while its remote object row does"
        );
    }
    assert!(
        surviving.len() < journal.operation().prepared_objects.len(),
        "discard must release every object payload it removed"
    );
}
