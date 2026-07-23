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

fn conflicted_membership_head(
    entry: &crate::sync::membership::MembershipEntry,
    registration: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    activation: crate::sync::store_commit::StreamActivationId,
    device_signer: &crate::keys::UserKeypair,
    label: &str,
) -> (
    crate::sync::membership::MembershipHeadRef,
    crate::sync::membership::AuthorHead,
) {
    let entry_bytes = serde_json::to_vec(entry).expect("serialize conflicted membership entry");
    let entry_ref = crate::sync::membership::MembershipEntryRef {
        coord: entry.coord(),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!(
                "test/conflicted-membership/{label}/entry.json"
            ))
            .expect("conflicted membership entry slot"),
            entry_bytes.len() as u64,
            ObjectHash::digest(&entry_bytes),
        ),
    };
    let head = crate::sync::membership::AuthorHead::signed(
        entry.store_id.clone(),
        crate::sync::membership::MembershipHeadBody {
            author_registration: registration.clone(),
            entry: entry_ref,
            predecessor: None,
            resolutions: entry.resolution_dependencies.clone(),
            successor: crate::sync::store_commit::SuccessorLink {
                activation,
                predecessor: None,
                next_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "test/conflicted-membership/{label}/next.json"
                ))
                .expect("conflicted membership successor slot"),
            },
        },
        crate::sync::membership::MembershipHeadActivation::Direct,
        device_signer,
    );
    let head_bytes = serde_json::to_vec(&head).expect("serialize conflicted membership head");
    let reference = crate::sync::membership::MembershipHeadRef {
        coord: entry.coord(),
        head_hash: head.head_hash(),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!(
                "test/conflicted-membership/{label}/head.json"
            ))
            .expect("conflicted membership head slot"),
            head_bytes.len() as u64,
            ObjectHash::digest(&head_bytes),
        ),
    };
    (reference, head)
}

#[tokio::test]
async fn pull_rejects_unresolved_membership_instead_of_treating_it_as_removal() {
    let database = open_scoped_replay_database();
    let owner = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &database,
        "unresolved-effective-access",
        owner.clone(),
    )
    .await
    .expect("create unresolved-membership Store");
    let mut membership = current_membership(&database, &store.storage).await;
    let target = crate::keys::UserKeypair::generate();
    let target_pubkey = crate::keys::public_key_hex(&target);
    let initial = membership
        .signed_set_member_in_stream(
            &owner,
            crate::sync::membership::AuthorStreamId::from_bytes([201; 32]),
            target_pubkey.clone(),
            None,
            crate::sync::membership::MemberRole::Member,
            "initial assignment".to_string(),
        )
        .expect("sign initial membership assignment");
    membership
        .add_entry(initial.clone())
        .expect("add initial membership assignment");
    let follower = membership
        .signed_set_member_in_stream(
            &owner,
            crate::sync::membership::AuthorStreamId::from_bytes([202; 32]),
            target_pubkey.clone(),
            None,
            crate::sync::membership::MemberRole::Follower,
            "concurrent Follower assignment".to_string(),
        )
        .expect("sign Follower assignment");
    let member = membership
        .signed_set_member_in_stream(
            &owner,
            crate::sync::membership::AuthorStreamId::from_bytes([203; 32]),
            target_pubkey,
            None,
            crate::sync::membership::MemberRole::Member,
            "concurrent Member assignment".to_string(),
        )
        .expect("sign Member assignment");
    let device_id = database
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load unresolved-membership device id")
        .expect("unresolved-membership device id exists");
    let (_, registration_ref, _, device_signer) =
        crate::sync::store::load_local_store_authority_for_test(
            &StoreDatabase::new(&database),
            &device_id,
            &owner,
        )
        .await
        .expect("load unresolved-membership author");
    let founder_ref = membership
        .head_refs()
        .first()
        .expect("load founder membership head")
        .clone();
    let founder_head = crate::sync::store::membership::load_exact_membership_head(
        &store.storage,
        &store.root,
        &founder_ref,
    )
    .await
    .expect("open founder membership head");
    let activation = founder_head.body.successor.activation;
    let mut entries = membership.entries().to_vec();
    entries.extend([follower.clone(), member.clone()]);
    let conflicted = MembershipChain::from_entries_with_coords_and_heads(
        entries
            .into_iter()
            .map(|entry| (entry.coord(), entry))
            .collect(),
        vec![
            (founder_ref, founder_head),
            conflicted_membership_head(
                &initial,
                &registration_ref,
                activation,
                &device_signer,
                "initial",
            ),
            conflicted_membership_head(
                &follower,
                &registration_ref,
                activation,
                &device_signer,
                "follower",
            ),
            conflicted_membership_head(
                &member,
                &registration_ref,
                activation,
                &device_signer,
                "member",
            ),
        ],
    )
    .expect("construct unresolved membership");
    assert!(conflicted.ensure_resolved().is_err());

    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let error = crate::sync::store::pull_store_commits(
        &StoreDatabase::new(&database),
        database.synced_tables(),
        &store.storage,
        store.root.store_root_hash,
        &store_dir,
        &conflicted,
        Some(&owner),
        Some(&crate::encryption::EncryptionService::from_key([42; 32])),
    )
    .await
    .expect_err("pull must reject unresolved Store membership");
    assert!(matches!(error, StorePullError::Membership(_)), "{error}");
}

#[tokio::test]
async fn active_store_member_holds_unavailable_circle_package_without_partial_materialization() {
    for failure in [PackageFailure::Missing, PackageFailure::Corrupt] {
        let member_database = open_scoped_replay_database();
        let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_member_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
        let fixture = effective_access_fixture(
            failure.store_id(),
            &member_database,
            &owner_store_dir,
            &member_store_dir,
        )
        .await;
        let first = publish_effective_access_row(
            &fixture,
            &owner_store_dir,
            "active baseline",
            "0000000002000-0000-owner",
        )
        .await;
        let membership =
            current_membership(&member_database, fixture.member_storage.as_ref()).await;
        let first_pull = pull_scoped_with(
            &member_database,
            &fixture.store,
            fixture.member_storage.as_ref(),
            &membership,
            &fixture.member,
            &member_store_dir,
        )
        .await
        .expect("pull unavailable-package baseline");
        assert!(
            first_pull.held_positions.is_empty(),
            "{failure:?}: {first_pull:?}"
        );

        let unavailable = publish_effective_access_row(
            &fixture,
            &owner_store_dir,
            "must not materialize",
            "0000000003000-0000-owner",
        )
        .await;
        let unavailable_commit = load_commit(&fixture, &unavailable).await;
        let unavailable_slot = exact_circle_package_slot(&unavailable_commit);
        match failure {
            PackageFailure::Missing => fixture.store.home.remove_exact_object(&unavailable_slot),
            PackageFailure::Corrupt => fixture
                .store
                .home
                .replace_exact_object(&unavailable_slot, b"corrupt Circle package".to_vec()),
        }
        fixture.store.home.clear_exact_reads();
        let pull = pull_scoped_with(
            &member_database,
            &fixture.store,
            fixture.member_storage.as_ref(),
            &membership,
            &fixture.member,
            &member_store_dir,
        )
        .await
        .expect("active member records unavailable private package as held");
        assert!(
            pull.held_positions
                .iter()
                .any(|held| held.coordinate.seq() == unavailable.coord.sequence()),
            "{failure:?}: {pull:?}"
        );
        assert!(fixture.store.home.exact_reads().contains(&unavailable_slot));
        let state = scoped_routing_state(&member_database, EFFECTIVE_ACCESS_ROW_ID).await;
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
