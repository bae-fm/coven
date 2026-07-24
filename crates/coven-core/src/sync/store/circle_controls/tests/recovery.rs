use super::*;

/// Prepare a founder Circle operation on a member device, then revoke that
/// member's grant so publishing it blocks under lost authority.
struct RevokedOperation {
    db: Database,
    store: TestStore,
    successor: UserKeypair,
    operation_id: CircleOperationId,
    author_grant_id: crate::sync::membership::MembershipGrantId,
}

async fn prepare_and_revoke_operation(name: &str) -> RevokedOperation {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(&db, name, &founder).await;
    let successor = UserKeypair::generate();
    let successor_pubkey = keys::public_key_hex(&successor);
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("founder-device".to_string()),
        &successor_pubkey,
        None,
        MemberRole::Member,
        &encryption,
        name,
        "Recovery test Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite successor member");
    let successor_db = open_test_db();
    install_active_device_fixture(
        &store,
        &db,
        &successor_db,
        &successor,
        "0000000001003-0000-successor",
    )
    .await
    .expect("activate successor device");
    let successor_device_id = local_device_id(&successor_db).await;
    // The member device is the one that authors and publishes; drive everything
    // through its database so its local membership view governs authority.
    let journal = prepare_circle_operation(
        &successor_db,
        &store.storage,
        &successor_device_id,
        "0000000001003-0000-successor",
        "Revoked Circle",
        &successor,
    )
    .await
    .expect("prepare operation while authorized");
    let author_grant_id = journal.operation().creation.control.value.author_grant_id();
    let operation_id = journal.operation_id.clone();
    StoreDatabase::new(&successor_db)
        .insert_circle_operation(journal)
        .await
        .expect("persist operation");

    let custody = TestCustody::default();
    custody.set_initial_key([42; 32]);
    let cipher = store.storage.cipher_state().clone();
    let pending_rotation = store.storage.shared_pending_rotation();
    crate::sync::store::membership::remove_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("founder-device".to_string()),
        &successor_pubkey,
        &encryption,
        &custody,
        cipher.as_ref(),
        pending_rotation.as_ref(),
        &StoreDatabase::new(&db),
    )
    .await
    .expect("remove successor grant");

    RevokedOperation {
        db: successor_db,
        store,
        successor,
        operation_id,
        author_grant_id,
    }
}

#[tokio::test]
async fn a_blocked_operation_reports_typed_authority_lost() {
    let revoked = prepare_and_revoke_operation("recovery-typed-block").await;
    resume_circle_operations(&revoked.db, &revoked.store.storage, &revoked.successor)
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
            block: crate::sync::circle::CircleOperationBlock::AuthorityLost {
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
            block: crate::sync::circle::CircleOperationBlock::AuthorityLost { grant_id }
        } if *grant_id == revoked.author_grant_id
    )));
}

#[tokio::test]
async fn retry_of_a_blocked_operation_republishes_its_exact_prepared_commit() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(&db, "recovery-retry-republish", &founder).await;
    let device_id = local_device_id(&db).await;
    let journal = prepare_circle_operation(
        &db,
        &store.storage,
        &device_id,
        "0000000001000-0000-founder",
        "Household",
        &founder,
    )
    .await
    .expect("prepare authorized founder operation");
    let operation_id = journal.operation_id.clone();
    let circle_id = journal.circle_id();
    let expected_control = journal.operation().creation.control.coord.clone();
    let expected_commit_object = journal.operation().commit_ref.object.clone();
    let author_grant_id = journal.operation().creation.control.value.author_grant_id();
    StoreDatabase::new(&db)
        .insert_circle_operation(journal)
        .await
        .expect("persist authorized operation");

    // The operation is durably blocked (its exact retained payload preserved),
    // then retried. Retry restores the phase and re-enters the publish pipeline
    // without regenerating anything.
    StoreDatabase::new(&db)
        .block_circle_operation(
            &operation_id,
            crate::sync::circle::CircleOperationBlock::AuthorityLost {
                grant_id: author_grant_id,
            },
        )
        .await
        .expect("block the authorized operation");
    retry_circle_operation(&db, &store.storage, &operation_id, &founder)
        .await
        .expect("retry publishes the still-authorized operation");

    assert!(StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read retried operation")
        .is_none());
    let (activated, activation_commit_ref) = StoreDatabase::new(&db)
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

#[tokio::test]
async fn retry_refuses_active_operations_and_reblocks_idempotently() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(&db, "recovery-retry-refusal", &founder).await;
    let device_id = local_device_id(&db).await;
    let ready = prepare_circle_operation(
        &db,
        &store.storage,
        &device_id,
        "0000000001000-0000-founder",
        "Household",
        &founder,
    )
    .await
    .expect("prepare an authorized operation");
    let ready_id = ready.operation_id.clone();
    StoreDatabase::new(&db)
        .insert_circle_operation(ready)
        .await
        .expect("persist the ready operation");
    let refusal = retry_circle_operation(&db, &store.storage, &ready_id, &founder)
        .await
        .expect_err("retrying an operation that is not blocked is refused");
    assert!(
        matches!(&refusal, CircleOperationError::NotBlocked { operation_id } if *operation_id == ready_id),
        "{refusal}"
    );

    // A permanently-blocked operation re-blocks on retry; retrying twice leaves it
    // durably blocked with no corruption.
    let revoked = prepare_and_revoke_operation("recovery-retry-reblock").await;
    resume_circle_operations(&revoked.db, &revoked.store.storage, &revoked.successor)
        .await
        .expect("resume blocks the revoked operation");
    for _ in 0..2 {
        match retry_circle_operation(
            &revoked.db,
            &revoked.store.storage,
            &revoked.operation_id,
            &revoked.successor,
        )
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
