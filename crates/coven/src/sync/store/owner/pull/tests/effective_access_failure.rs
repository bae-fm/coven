use super::*;

#[derive(Clone, Copy, Debug)]
enum PackageFailure {
    Missing,
    Corrupt,
}

impl PackageFailure {
    fn store_id(self) -> &'static str {
        match self {
            Self::Missing => "active-member-missing-package-rollback",
            Self::Corrupt => "active-member-corrupt-package-rollback",
        }
    }
}

#[tokio::test]
async fn pull_rejects_unresolved_membership_instead_of_treating_it_as_removal() {
    let owner_database = open_scoped_replay_database();
    let owner = coven_keys::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_database,
        "unresolved-effective-access",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create unresolved-membership Store");
    let second_owner = coven_keys::keys::UserKeypair::generate();
    let second_owner_database = open_scoped_replay_database();
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    store
        .invite_member(
            &owner_database,
            &owner,
            &coven_keys::keys::public_key_hex(&second_owner),
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Unresolved Membership Store",
        )
        .await
        .expect("invite the second owner");
    store
        .activate_joined_device(
            &owner_database,
            &second_owner_database,
            &second_owner,
            "2026-07-23T00:00:00Z",
        )
        .await
        .expect("activate the second owner's device");
    store
        .promote_active_member_fixture(
            &owner_database,
            &second_owner_database,
            &owner,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote the second owner");

    let owner_store = store
        .bind_device(&owner_database, &owner)
        .await
        .expect("bind the founder");
    let second_owner_store = store
        .bind_device(&second_owner_database, &second_owner)
        .await
        .expect("bind the second owner");
    let mut founder_writer = owner_store
        .authorize_writer()
        .await
        .expect("authorize the founder before either assignment");
    let mut second_writer = second_owner_store
        .authorize_writer()
        .await
        .expect("authorize the second owner before either assignment");
    let target = coven_keys::keys::UserKeypair::generate();
    let target_pubkey = coven_keys::keys::public_key_hex(&target);
    founder_writer
        .invite_member(
            &target_pubkey,
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "unresolved-effective-access",
            "Unresolved Membership Store",
        )
        .await
        .expect("publish the founder's assignment");
    second_writer
        .invite_member(
            &target_pubkey,
            None,
            crate::protocol::membership::MemberRole::Follower,
            &encryption,
            "unresolved-effective-access",
            "Unresolved Membership Store",
        )
        .await
        .expect("publish the second owner's conflicting assignment");
    drop(founder_writer);
    drop(second_writer);

    let mut founder_writer = owner_store
        .authorize_writer()
        .await
        .expect("authorize the founder with the unresolved membership");
    let error = founder_writer
        .pull(Some(&encryption))
        .await
        .expect_err("pull must reject unresolved Store membership");
    assert!(error.to_string().contains("membership"), "{error}");
}

#[tokio::test]
async fn active_store_member_holds_unavailable_circle_package_without_partial_materialization() {
    for failure in [PackageFailure::Missing, PackageFailure::Corrupt] {
        let member_database = open_scoped_replay_database();
        let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_member_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
        let fixture = EffectiveAccessFixture::create(
            failure.store_id(),
            &member_database,
            &owner_store_dir,
            &member_store_dir,
        )
        .await;
        let first = fixture
            .publish_row(
                EFFECTIVE_ACCESS_ROW_ID,
                "active baseline",
                "0000000002000-0000-owner",
            )
            .await;
        let first_pull = fixture
            .pull_member(&member_store_dir)
            .await
            .expect("pull unavailable-package baseline");
        assert!(
            first_pull.held_positions.is_empty(),
            "{failure:?}: {first_pull:?}"
        );

        let unavailable = fixture
            .publish_row(
                EFFECTIVE_ACCESS_ROW_ID,
                "must not materialize",
                "0000000003000-0000-owner",
            )
            .await;
        let unavailable_commit = fixture.load_commit(&unavailable).await;
        let unavailable_slot = exact_circle_package_slot(&unavailable_commit);
        match failure {
            PackageFailure::Missing => fixture.home.remove_exact_object(&unavailable_slot),
            PackageFailure::Corrupt => fixture
                .home
                .replace_exact_object(&unavailable_slot, b"corrupt Circle package".to_vec()),
        }
        fixture.home.clear_exact_reads();
        let pull = fixture
            .pull_member(&member_store_dir)
            .await
            .expect("active member records unavailable private package as held");
        assert!(
            pull.held_positions
                .iter()
                .any(|held| held.coordinate.seq() == unavailable.coord.sequence()),
            "{failure:?}: {pull:?}"
        );
        assert!(fixture.home.exact_reads().contains(&unavailable_slot));
        let state = member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await;
        assert_eq!(
            state.row.as_ref().map(|row| row.1.as_str()),
            Some("active baseline"),
            "{failure:?}"
        );
        assert_eq!(
            state.mirror.as_ref().map(|mirror| mirror.1.as_str()),
            Some("0000000002000-0000-owner"),
            "{failure:?}"
        );
        assert_eq!(
            StoreDatabase::new(&member_database)
                .exact_materialized_ref(&commit_stream_id(&first.coord), first.coord.sequence())
                .await
                .expect("load unavailable-package baseline position"),
            Some(first),
            "{failure:?}"
        );
        assert!(
            StoreDatabase::new(&member_database)
                .exact_materialized_ref(
                    &commit_stream_id(&unavailable.coord),
                    unavailable.coord.sequence(),
                )
                .await
                .expect("check unavailable package position")
                .is_none(),
            "{failure:?}"
        );
    }
}
