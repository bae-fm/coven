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
async fn a_conflicting_admission_preserves_the_accepted_grant_and_pull_access() {
    let owner_database_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_database = open_scoped_replay_database(owner_database_store_dir.clone());
    let owner = coven_keys::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_database,
        owner_database_store_dir.clone(),
        "conflicting-effective-access",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create conflicting-assignment Store");
    let second_owner = coven_keys::keys::UserKeypair::generate();
    let second_owner_database_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_database =
        open_scoped_replay_database(second_owner_database_store_dir.clone());
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    store
        .admit_member(
            &owner_database,
            owner_database_store_dir.clone(),
            &owner,
            &coven_keys::keys::public_key_hex(&second_owner),
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Conflicting Assignment Store",
        )
        .await
        .expect("admit the second owner");
    store
        .activate_joined_device(
            &owner_database,
            owner_database_store_dir.clone(),
            &second_owner_database,
            second_owner_database_store_dir.clone(),
            &second_owner,
            "2026-07-23T00:00:00Z",
        )
        .await
        .expect("activate the second owner's device");
    store
        .promote_active_member_fixture(
            &owner_database,
            owner_database_store_dir.clone(),
            &second_owner_database,
            second_owner_database_store_dir.clone(),
            &owner,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote the second owner");

    let owner_store = store
        .bind_device_in(&owner_database, owner_database_store_dir.clone(), &owner)
        .await
        .expect("bind the founder");
    let second_owner_store = store
        .bind_device_in(
            &second_owner_database,
            second_owner_database_store_dir.clone(),
            &second_owner,
        )
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
        .admit_member(
            &target_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "conflicting-effective-access",
            "Conflicting Assignment Store",
        )
        .await
        .expect("publish the founder's assignment");
    let accepted = StoreDatabase::new(&owner_database)
        .store_current_publication()
        .await
        .expect("accepted first assignment");
    let original_grants = owner_store
        .membership_for_test()
        .await
        .expect("accepted target membership")
        .active_grant_ids(&target_pubkey);
    assert_eq!(original_grants.len(), 1);
    let error = second_writer
        .admit_member(
            &target_pubkey,
            None,
            coven_protocol::membership::MemberRole::Follower,
            &encryption,
            "conflicting-effective-access",
            "Conflicting Assignment Store",
        )
        .await
        .expect_err("an earlier writer handle cannot overwrite the accepted assignment");
    assert!(matches!(
        error,
        crate::sync::store::membership::MembershipOpsError::ExistingMemberMismatch
    ));
    let second_database = StoreDatabase::new(&second_owner_database);
    assert!(second_database
        .outbound_membership_mutation()
        .await
        .expect("rejected assignment journal")
        .is_none());
    assert!(second_database
        .active_store_publication()
        .await
        .expect("rejected assignment reservation")
        .is_none());
    drop(founder_writer);
    drop(second_writer);

    let (_, pulled) = owner_store
        .pull_store()
        .await
        .expect("rejected conflicting assignment leaves accepted history readable");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        StoreDatabase::new(&owner_database)
            .store_current_publication()
            .await
            .expect("boundary after rejected assignment"),
        accepted
    );
    let membership = owner_store
        .membership_for_test()
        .await
        .expect("accepted membership");
    assert!(membership.conflict().is_none());
    assert_eq!(membership.active_grant_ids(&target_pubkey), original_grants);
    assert!(membership.current_members().contains(&(
        target_pubkey,
        coven_protocol::membership::MemberRole::Member,
    )));
}

#[tokio::test]
async fn active_store_member_holds_unavailable_circle_package_without_partial_materialization() {
    for failure in [PackageFailure::Missing, PackageFailure::Corrupt] {
        let member_database_store_dir = crate::sync::test_helpers::test_store_dir();
        let member_database = open_scoped_replay_database(member_database_store_dir.clone());
        let fixture = EffectiveAccessFixture::create(
            failure.store_id(),
            &member_database,
            member_database_store_dir.clone(),
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
            .pull_member()
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
            .pull_member()
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
