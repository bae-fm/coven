use super::*;

#[tokio::test]
async fn deleting_a_circle_prunes_receivers_and_refuses_new_writes() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let (member_database, member_database_store_dir) =
        open_scoped_replay_database_at(&member_path, "scoped-replay-device");
    let fixture = EffectiveAccessFixture::create(
        "delete-circle-prunes",
        &member_database,
        member_database_store_dir.clone(),
    )
    .await;

    let commit_ref = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "before deletion",
            "0000000002000-0000-owner",
        )
        .await;
    fixture
        .pull_member()
        .await
        .expect("member pulls the pre-deletion Circle row");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("before deletion")
    );

    // The Circle row carries a blob. Both the owner (host author) and the member
    // (recipient) hold a `row_blob_locators` binding for it, which the deletion
    // must prune along with the row.
    let commit = fixture.load_commit(&commit_ref).await;
    let [package] = commit.circle_packages() else {
        panic!("Circle row has one accepted package");
    };
    let bytes = b"Circle row attachment";
    let locator = coven_protocol::blob::locator::BlobLocator::opaque(
        "attachments",
        EFFECTIVE_ACCESS_ROW_ID,
        commit.value().author_registration.clone(),
        coven_protocol::blob::locator::RemoteAudience::Circle(fixture.circle_id),
        coven_protocol::blob::BlobScope::Master,
        package.key_fingerprint,
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
    .expect("construct Circle blob locator");
    let object = coven_protocol::objects::ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(locator.semantic_key())
            .expect("construct Circle blob slot"),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    );
    let binding = coven_protocol::audience_package::RowBlobLocatorBinding::new(
        "notes",
        EFFECTIVE_ACCESS_ROW_ID,
        "0000000002000-0000-owner",
        "attachment",
        coven_protocol::blob::locator::StoredBlobRef::new(locator, object)
            .expect("construct stored Circle blob"),
    )
    .expect("construct Circle row binding");
    for database in [&fixture.owner_database, &member_database] {
        database
            .bind_circle_row_blob_for_test(binding.clone(), package.clone(), commit_ref.clone())
            .await;
    }
    assert_eq!(
        fixture
            .owner_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        1,
        "the owner holds the Circle row's blob binding before deletion"
    );
    assert_eq!(
        member_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        1,
        "the member holds the Circle row's blob binding before deletion"
    );

    // A pre-deletion Circle package the member has not yet pulled.
    fixture
        .publish_row(
            READD_EFFECTIVE_ACCESS_ROW_ID,
            "private before deletion",
            "0000000002500-0000-owner",
        )
        .await;

    fixture.delete_circle().await;

    // The owner converges to Deleted: rows pruned, control spine retained.
    assert!(fixture
        .owner_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await
        .row
        .is_none());
    assert!(
        fixture
            .owner_database
            .circle_control_activation_count_for_test(fixture.circle_id)
            .await
            > 0,
        "the owner retains the control authority spine after deletion"
    );
    assert_eq!(
        fixture
            .owner_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        0,
        "the owner's Circle row blob binding is pruned on deletion"
    );
    let owner_circles = StoreDatabase::new(&fixture.owner_database)
        .get_circles(
            &coven_keys::keys::public_key_hex(&fixture.owner),
            fixture.effective_access_members(),
        )
        .await
        .expect("list owner Circles after deletion");
    assert!(
        matches!(owner_circles.as_slice(),
            [coven_protocol::circle::CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the owner reports the Circle as deleted: {owner_circles:?}"
    );

    // The member pulls the deletion (and the late pre-deletion package) and
    // converges identically: rows, routes, and the late package are gone.
    fixture
        .pull_member()
        .await
        .expect("member pulls the deletion");
    let pruned = member_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert!(pruned.row.is_none(), "the member's Circle row is pruned");
    assert!(
        pruned.route.is_none(),
        "the member's private route is pruned"
    );
    assert!(
        member_database
            .scoped_routing_state_for_test(READD_EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .is_none(),
        "the late pre-deletion package is omitted"
    );
    assert!(
        member_database
            .circle_control_activation_count_for_test(fixture.circle_id)
            .await
            > 0,
        "the member retains the control authority spine after deletion"
    );
    assert_eq!(
        member_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        0,
        "the member's Circle row blob binding is pruned on deletion"
    );
    let member_circles = StoreDatabase::new(&member_database)
        .get_circles(
            &coven_keys::keys::public_key_hex(&fixture.member),
            fixture.effective_access_members(),
        )
        .await
        .expect("list member Circles after deletion");
    assert!(
        matches!(member_circles.as_slice(),
            [coven_protocol::circle::CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the member reports the Circle as deleted: {member_circles:?}"
    );

    // A new host write destined to the deleted Circle is refused at capture.
    let circle_id = fixture.circle_id;
    let error = StoreDatabase::new(&fixture.owner_database)
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |transaction| {
                transaction
                    .execute_batch(&format!(
                        "INSERT INTO notes (id, audience, body, _updated_at)
                             VALUES ('01890a5d-ac96-774b-bcce-b302099c3f99', '{circle_id}',
                                     'after deletion', '0000000003000-0000-owner');"
                    ))
                    .map_err(DbError::from)
            },
        )
        .await
        .expect_err("a host write into a deleted Circle is refused");
    assert!(error.to_string().contains("deleted"), "{error}");
}
