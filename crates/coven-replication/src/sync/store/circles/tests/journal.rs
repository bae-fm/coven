use super::*;

#[tokio::test]
async fn circle_preparation_leaves_payload_installation_to_the_database() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture = create_test_store_fixture_in_its_own_task(
        &db,
        db_store_dir.clone(),
        "circle-payload-owner",
        &signer,
        home,
    )
    .await;
    let (store, _connection) = fixture;
    let prepared = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-creator", "Household")
        .await
        .expect("prepare Circle operation");
    let hashes = prepared
        .prepared_objects
        .values()
        .map(|object| object.reference().stored_hash())
        .collect::<std::collections::BTreeSet<_>>();

    for hash in &hashes {
        assert!(
            !StoreDatabase::new(&db)
                .has_payload_for_test(*hash)
                .await
                .expect("check uninstalled prepared payload"),
            "preparation installed payload {hash} without a durable owner"
        );
    }

    let operation_id = prepared.journal.operation_id.clone();
    StoreDatabase::new(&db)
        .insert_circle_operation(prepared.journal, prepared.prepared_objects)
        .await
        .expect("persist Circle operation");
    let claims = StoreDatabase::new(&db)
        .circle_operation_payload_claims_for_test(&operation_id)
        .await
        .expect("read Circle operation payload claims")
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(claims, hashes);
    let active = StoreDatabase::new(&db)
        .active_store_publication()
        .await
        .expect("read active Store publication")
        .expect("Circle operation must reserve Store publication with its journal");
    assert_eq!(
        active.owner(),
        &coven_database::ActiveStorePublicationOwner::CircleOperation(operation_id)
    );
}

#[tokio::test]
async fn circle_operation_lookup_rejects_a_payload_with_another_operation_id() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (_store, _home, _signer, journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-operation-id-mismatch").await;
    let expected_operation_id = journal.operation_id.clone();
    let replacement_write_id =
        coven_protocol::write::WriteId::from_generated("another-circle-operation".to_string());
    let mut replacement = journal.clone();
    replacement.operation_id = CircleOperationId::from_write_id(replacement_write_id.clone());
    replacement
        .operation_mut()
        .store_commit
        .common
        .commit
        .body_mut()
        .write_id = replacement_write_id;
    db.replace_circle_operation_prepared_for_test(expected_operation_id, replacement)
        .await
        .expect("install mismatched Circle operation payload");

    let error = coven_database::StoreDatabase::new(&db)
        .circle_operation(&journal.operation_id)
        .await
        .expect_err("lookup authority must match the payload operation id");
    assert!(error.to_string().contains("operation id"), "{error}");
}

#[tokio::test]
async fn circle_operation_lookup_rejects_a_payload_with_another_circle_id() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (_store, _home, _signer, journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-id-mismatch").await;
    let expected_operation_id = journal.operation_id.clone();
    let replacement_circle_id = CircleId::from_bytes([7; 16]);
    let mut replacement = journal.clone();
    replacement.circle_id = replacement_circle_id;
    replacement.operation_mut().creation.circle_id = replacement_circle_id;
    db.replace_circle_operation_prepared_for_test(expected_operation_id, replacement)
        .await
        .expect("install mismatched Circle operation payload");

    let error = coven_database::StoreDatabase::new(&db)
        .circle_operation(&journal.operation_id)
        .await
        .expect_err("lookup authority must match the payload Circle id");
    assert!(error.to_string().contains("payload circle id"), "{error}");
}

#[tokio::test]
async fn blocking_a_circle_operation_targets_its_exact_operation_id() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (_store, _home, _signer, first) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-block-first").await;
    let absent_operation_id = CircleOperationId::from_write_id(
        coven_protocol::write::WriteId::from_generated("absent-circle-operation".to_string()),
    );

    let error = coven_database::StoreDatabase::new(&db)
        .block_circle_operation(
            &absent_operation_id,
            coven_protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: coven_protocol::membership::MembershipGrantId(
                    coven_protocol::store_commit::ObjectHash::digest(b"absent revoked grant"),
                ),
            },
        )
        .await
        .expect_err("blocking requires the exact durable operation id");
    assert!(error.to_string().contains("is absent"), "{error}");

    coven_database::StoreDatabase::new(&db)
        .block_circle_operation(
            &first.operation_id,
            coven_protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: coven_protocol::membership::MembershipGrantId(
                    coven_protocol::store_commit::ObjectHash::digest(b"revoked grant"),
                ),
            },
        )
        .await
        .expect("block first Circle operation");

    let first = coven_database::StoreDatabase::new(&db)
        .circle_operation(&first.operation_id)
        .await
        .expect("read first Circle operation")
        .expect("first Circle operation remains durable");
    assert!(matches!(
        first.state(),
        CircleOperationState::Blocked { .. }
    ));
}

#[tokio::test]
async fn publishing_a_circle_operation_targets_its_exact_operation_id() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-publish-id").await;
    let absent_operation_id = CircleOperationId::from_write_id(
        coven_protocol::write::WriteId::from_generated("absent-circle-operation".to_string()),
    );

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .publish_circle_operation(&absent_operation_id)
        .await
        .expect_err("publication requires the exact durable operation id");

    assert!(
        matches!(error, CircleOperationError::JournalState(_)),
        "{error}"
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .circle_operation(&journal.operation_id)
            .await
            .expect("read exact Circle operation")
            .expect("exact Circle operation remains durable")
            .state(),
        CircleOperationState::Pending
    );
}

#[tokio::test]
async fn circle_creation_retains_its_author_turn_until_the_candidate_is_durable() {
    use futures_util::FutureExt;

    let store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(store_dir.clone());
    let signer = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        store_dir.clone(),
        "circle-durable-author-turn",
        &signer,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let owner = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind Circle owner");
    let database = StoreDatabase::new(&db);
    let before = database
        .store_current_publication()
        .await
        .expect("read initial publication");
    let (prepared, resume) =
        database.arm_test_pause(coven_database::DatabaseTestPoint::CircleCandidatePrepared);
    let creation = owner.create_circle("0000000001000-0000-creator", "Household");
    tokio::pin!(creation);
    tokio::select! {
        _ = prepared.notified() => {},
        result = &mut creation => panic!("Circle creation ended before candidate staging: {result:?}"),
    }
    assert!(database
        .oldest_pending_circle_operation()
        .await
        .expect("read unstaged Circle journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("read publication reservation")
        .is_none());
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read paused publication"),
        before
    );
    assert!(
        database.author_own_stream().now_or_never().is_none(),
        "another local author must not take the captured position before the Circle candidate is durable"
    );
    resume.notify_one();
    let circle_id = creation.await.expect("stage and publish Circle creation");
    assert!(
        database.author_own_stream().now_or_never().is_some(),
        "Circle completion releases the author turn"
    );
    assert!(database
        .active_store_publication()
        .await
        .expect("read completed publication reservation")
        .is_none());
    assert!(database
        .oldest_pending_circle_operation()
        .await
        .expect("read completed Circle journal")
        .is_none());
    assert_eq!(
        database
            .circle_control_activation_count_for_test(circle_id)
            .await
            .expect("count accepted Circle control"),
        1
    );
}
