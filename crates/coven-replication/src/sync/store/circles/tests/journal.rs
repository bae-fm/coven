use super::*;

#[tokio::test]
async fn circle_preparation_leaves_payload_installation_to_the_database() {
    let db = open_test_db();
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture =
        create_test_store_fixture_in_its_own_task(&db, "circle-payload-owner", &signer, home).await;
    let prepared = fixture
        .store
        .bind_device(&db, &signer)
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
            !StoreDatabase::new(&db.database)
                .has_payload_for_test(*hash)
                .await
                .expect("check uninstalled prepared payload"),
            "preparation installed payload {hash} without a durable owner"
        );
    }

    let operation_id = prepared.journal.operation_id.clone();
    StoreDatabase::new(&db.database)
        .insert_circle_operation(prepared.journal, prepared.prepared_objects)
        .await
        .expect("persist Circle operation");
    let claims = StoreDatabase::new(&db.database)
        .circle_operation_payload_claims_for_test(&operation_id)
        .await
        .expect("read Circle operation payload claims")
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(claims, hashes);
}

#[tokio::test]
async fn circle_operation_lookup_rejects_a_payload_with_another_operation_id() {
    let db = open_test_db();
    let (_store, _home, _signer, journal) =
        persist_merge_operation(&db, "circle-operation-id-mismatch").await;
    let expected_operation_id = journal.operation_id.clone();
    let replacement_write_id =
        coven_protocol::write::WriteId::from_generated("another-circle-operation".to_string());
    let mut replacement = journal.clone();
    replacement.operation_id = CircleOperationId::from_write_id(replacement_write_id.clone());
    let mut replacement_commit = replacement.commit().expect("parse replacement commit");
    replacement_commit.body_mut().write_id = replacement_write_id;
    replacement.operation_mut().commit_bytes =
        serde_json::to_vec(&replacement_commit).expect("serialize replacement commit");
    db.database
        .replace_circle_operation_prepared_for_test(expected_operation_id, replacement)
        .await
        .expect("install mismatched Circle operation payload");

    let error = coven_database::StoreDatabase::new(&db.database)
        .circle_operation(&journal.operation_id)
        .await
        .expect_err("lookup authority must match the payload operation id");
    assert!(error.to_string().contains("operation id"), "{error}");
}

#[tokio::test]
async fn circle_operation_lookup_rejects_a_payload_with_another_circle_id() {
    let db = open_test_db();
    let (_store, _home, _signer, journal) =
        persist_merge_operation(&db, "circle-id-mismatch").await;
    let expected_operation_id = journal.operation_id.clone();
    let replacement_circle_id = CircleId::from_bytes([7; 16]);
    let mut replacement = journal.clone();
    replacement.circle_id = replacement_circle_id;
    replacement.operation_mut().creation.circle_id = replacement_circle_id;
    db.database
        .replace_circle_operation_prepared_for_test(expected_operation_id, replacement)
        .await
        .expect("install mismatched Circle operation payload");

    let error = coven_database::StoreDatabase::new(&db.database)
        .circle_operation(&journal.operation_id)
        .await
        .expect_err("lookup authority must match the payload Circle id");
    assert!(error.to_string().contains("payload circle id"), "{error}");
}

#[tokio::test]
async fn blocking_a_circle_operation_targets_its_exact_operation_id() {
    let db = open_test_db();
    let (store, _home, signer, first) = persist_merge_operation(&db, "circle-block-first").await;
    let second = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000002000-0000-creator", "Second household")
        .await
        .expect("prepare second Circle operation");
    coven_database::StoreDatabase::new(&db.database)
        .insert_circle_operation(second.journal.clone(), second.prepared_objects)
        .await
        .expect("persist second Circle operation");
    let second = second.journal;

    coven_database::StoreDatabase::new(&db.database)
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

    let first = coven_database::StoreDatabase::new(&db.database)
        .circle_operation(&first.operation_id)
        .await
        .expect("read first Circle operation")
        .expect("first Circle operation remains durable");
    let second = coven_database::StoreDatabase::new(&db.database)
        .circle_operation(&second.operation_id)
        .await
        .expect("read second Circle operation")
        .expect("second Circle operation remains durable");
    assert!(matches!(
        first.state(),
        CircleOperationState::Blocked { .. }
    ));
    assert_eq!(second.state(), CircleOperationState::Pending);
}

#[tokio::test]
async fn publishing_a_circle_operation_targets_its_exact_operation_id() {
    let db = open_test_db();
    let (store, _home, signer, journal) = persist_merge_operation(&db, "circle-publish-id").await;
    let absent_operation_id = CircleOperationId::from_write_id(
        coven_protocol::write::WriteId::from_generated("absent-circle-operation".to_string()),
    );

    let error = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .publish_circle_operation(&absent_operation_id)
        .await
        .expect_err("publication requires the exact durable operation id");

    assert!(matches!(error, CircleOperationError::Journal(_)), "{error}");
    assert_eq!(
        coven_database::StoreDatabase::new(&db.database)
            .circle_operation(&journal.operation_id)
            .await
            .expect("read exact Circle operation")
            .expect("exact Circle operation remains durable")
            .state(),
        CircleOperationState::Pending
    );
}
