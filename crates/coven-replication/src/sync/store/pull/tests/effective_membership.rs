use super::*;

#[test]
fn later_removal_blocks_historical_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Removed,
            LocalStoreMembership::Current,
        ),
        LocalStoreMembership::Removed
    );
}

#[test]
fn later_admission_does_not_grant_pre_admission_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Current,
            LocalStoreMembership::NotYetMember,
        ),
        LocalStoreMembership::NotYetMember
    );
}

#[test]
fn later_readd_does_not_grant_removed_interval_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Current,
            LocalStoreMembership::Removed,
        ),
        LocalStoreMembership::Removed
    );
}

#[tokio::test]
async fn newly_discovered_store_admission_activates_circle_access() {
    let member_database_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_database = open_scoped_replay_database(member_database_store_dir.clone());
    let fixture = EffectiveAccessFixture::create(
        "newly-admitted-member-effective-access",
        &member_database,
        member_database_store_dir.clone(),
    )
    .await;

    assert_eq!(
        StoreDatabase::new(&member_database)
            .get_circles(
                &coven_keys::keys::public_key_hex(&fixture.member),
                std::collections::BTreeSet::from([
                    coven_keys::keys::public_key_hex(&fixture.owner),
                    coven_keys::keys::public_key_hex(&fixture.member),
                ]),
            )
            .await
            .expect("list Circles after newly discovered Store admission")
            .into_iter()
            .map(|circle| circle.name().expect("listed Circle is active").to_string())
            .collect::<Vec<_>>(),
        vec!["Effective Access".to_string()]
    );
}

#[tokio::test]
async fn removed_store_member_skips_late_circle_package_and_atomically_prunes_rows() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let (member_database, member_database_store_dir) =
        open_scoped_replay_database_at(&member_path, "scoped-replay-device");
    let fixture = EffectiveAccessFixture::create(
        "removed-member-effective-access",
        &member_database,
        member_database_store_dir.clone(),
    )
    .await;

    let first = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "visible before removal",
            "0000000002000-0000-owner",
        )
        .await;
    let first_pull = fixture
        .pull_member()
        .await
        .expect("pull pre-removal Circle row");
    assert!(first_pull.held_positions.is_empty(), "{first_pull:?}");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible before removal")
    );
    let hidden_before_removal = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private immediately before removal",
            "0000000002500-0000-owner",
        )
        .await;
    let hidden_before_removal_commit = fixture.load_commit(&hidden_before_removal).await;
    let hidden_before_removal_package_slot =
        exact_circle_package_slot(&hidden_before_removal_commit);

    // The last Circle package the owner authors before the removal. Once the
    // removal is materialized the owner may no longer publish new Circle content
    // (the Circle is rotation-required), so this models the newest package the
    // removed member must still be pruned from.
    let late = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private just before removal",
            "0000000002800-0000-owner",
        )
        .await;
    let late_commit = fixture.load_commit(&late).await;
    let late_package_slot = exact_circle_package_slot(&late_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    fixture
        .store
        .remove_member(
            &fixture.owner_database,
            fixture.owner_database_store_dir.clone(),
            &fixture.owner,
            &coven_keys::keys::public_key_hex(&fixture.member),
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            &custody,
        )
        .await
        .expect("remove effective-access Store member");
    let removal = fixture
        .owner_device
        .latest_local_store_position()
        .await
        .expect("load Store removal position")
        .expect("Store removal has a position");
    let latest_membership = fixture
        .member_device
        .membership()
        .await
        .expect("load current removed-member Store membership");
    assert!(!latest_membership
        .current_members()
        .iter()
        .any(|(member, _)| member == &coven_keys::keys::public_key_hex(&fixture.member)));

    fixture.home.clear_exact_reads();
    member_database.fail_next_merge_materialization_at(
        coven_database::MergeMaterializationFailurePoint::SummaryMaterialization,
    );
    fixture
        .pull_member()
        .await
        .expect_err("injected transaction failure interrupts removed-member materialization");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible before removal")
    );
    assert!(StoreDatabase::new(&member_database)
        .exact_materialized_ref(&commit_stream_id(&late.coord), late.coord.sequence(),)
        .await
        .expect("check rolled-back late position")
        .is_none());

    fixture
        .home
        .remove_exact_object(&hidden_before_removal_package_slot);
    fixture.home.remove_exact_object(&late_package_slot);
    fixture.home.clear_exact_reads();
    let pull = fixture
        .pull_member()
        .await
        .expect("pull Store state after membership removal");
    assert!(pull.held_positions.is_empty(), "{pull:?}");
    assert!(!fixture
        .home
        .exact_reads()
        .contains(&hidden_before_removal_package_slot));
    assert!(!fixture.home.exact_reads().contains(&late_package_slot));
    let state = member_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert_eq!(state.row, None);
    assert!(StoreDatabase::new(&member_database)
        .get_circles(
            &coven_keys::keys::public_key_hex(&fixture.member),
            std::collections::BTreeSet::from([coven_keys::keys::public_key_hex(&fixture.owner)]),
        )
        .await
        .expect("list Circles after Store membership removal")
        .is_empty());
    assert!(StoreDatabase::new(&member_database)
        .circle_authoring_context(
            fixture.circle_id,
            &coven_keys::keys::public_key_hex(&fixture.member),
        )
        .await
        .is_err());
    let (public_circle_state, private_circle_state): (i64, i64) = member_database
        .circle_state_table_counts_for_test()
        .await
        .expect("count Circle state after Store membership removal");
    assert_eq!(public_circle_state, 1);
    assert_eq!(private_circle_state, 0);
    assert_eq!(
        state.mirror,
        Some((
            Some(fixture.circle_id.to_string()),
            "0000000002000-0000-owner".to_string(),
        ))
    );
    for reference in [&first, &hidden_before_removal, &late, &removal] {
        assert_eq!(
            StoreDatabase::new(&member_database)
                .exact_materialized_ref(
                    &commit_stream_id(&reference.coord),
                    reference.coord.sequence(),
                )
                .await
                .expect("load effective-access materialized position"),
            Some(reference.clone())
        );
    }

    let circle_id = fixture.circle_id;
    let member_pubkey = coven_keys::keys::public_key_hex(&fixture.member);
    let owner_pubkey = coven_keys::keys::public_key_hex(&fixture.owner);
    let member_device_id = fixture.member_device.device_id();
    drop(fixture);
    std::thread::spawn(move || drop(member_database))
        .join()
        .expect("close effective-access member database");
    let (reopened, _reopened_store_dir) =
        open_scoped_replay_database_at(&member_path, &member_device_id);
    let reopened_state = reopened
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert_eq!(reopened_state.row, None);
    assert_eq!(
        reopened_state.mirror,
        Some((
            Some(circle_id.to_string()),
            "0000000002000-0000-owner".to_string(),
        ))
    );
    assert_eq!(
        StoreDatabase::new(&reopened)
            .exact_materialized_ref(&commit_stream_id(&removal.coord), removal.coord.sequence(),)
            .await
            .expect("load reopened removal position"),
        Some(removal)
    );
    assert!(StoreDatabase::new(&reopened)
        .get_circles(
            &member_pubkey,
            std::collections::BTreeSet::from([owner_pubkey]),
        )
        .await
        .expect("list reopened Circles after Store membership removal")
        .is_empty());
    let reopened_public_circle_state: i64 = reopened
        .table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "circle_current_state",
        ))
        .await
        .expect("count reopened public Circle state");
    assert_eq!(reopened_public_circle_state, 1);
}

#[tokio::test]
async fn readded_store_member_restores_circle_access_from_a_stale_removed_membership() {
    assert_readded_member_access(false).await;
}

#[tokio::test]
async fn a_late_commit_from_the_removed_interval_cannot_prune_readmitted_circle_access() {
    assert_readded_member_access(true).await;
}

async fn assert_readded_member_access(publish_removed_interval_write: bool) {
    let member_database_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_database = open_scoped_replay_database(member_database_store_dir.clone());
    let fixture = EffectiveAccessFixture::create(
        "readded-member-effective-access",
        &member_database,
        member_database_store_dir.clone(),
    )
    .await;

    let peer = if publish_removed_interval_write {
        let directory = crate::sync::test_helpers::test_store_dir();
        let database = open_scoped_replay_database(directory.clone());
        let device = fixture
            .store
            .activate_joined_device(
                &fixture.owner_database,
                fixture.owner_database_store_dir.clone(),
                &database,
                directory,
                &fixture.owner,
                "2026-07-23T00:00:00Z",
            )
            .await
            .expect("activate a concurrent owner device");
        Some((database, device))
    } else {
        None
    };

    fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "visible before removal",
            "0000000002000-0000-owner",
        )
        .await;
    let initial_pull = fixture
        .pull_member()
        .await
        .expect("pull Circle row before Store removal");
    assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");

    // A Circle package the owner authors before the removal that the member has
    // not yet pulled. The removal pull applies it under the removed membership,
    // exercising the prune of the member's Circle rows.
    let pre_removal = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private just before removal",
            "0000000002500-0000-owner",
        )
        .await;
    let pre_removal_commit = fixture.load_commit(&pre_removal).await;
    let pre_removal_package_slot = exact_circle_package_slot(&pre_removal_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    fixture
        .store
        .remove_member(
            &fixture.owner_database,
            fixture.owner_database_store_dir.clone(),
            &fixture.owner,
            &coven_keys::keys::public_key_hex(&fixture.member),
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            &custody,
        )
        .await
        .expect("remove Store member before re-add");
    // Once the removal is materialized the owner can no longer publish new
    // Circle content (the Circle is rotation-required until it is closed and
    // rotated), so no package is authored during the removed interval; the
    // re-add restores access to the Circle's current state alone.
    fixture.home.clear_exact_reads();
    let removal_pull = fixture
        .pull_member()
        .await
        .expect("pull Store membership removal");
    assert!(removal_pull.held_positions.is_empty(), "{removal_pull:?}");
    assert!(
        !fixture
            .home
            .exact_reads()
            .contains(&pre_removal_package_slot),
        "a removed member does not fetch the unpulled pre-removal Circle package"
    );
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row,
        None
    );

    let rotated_store_encryption = coven_keys::encryption::EncryptionService::from(
        custody
            .unlock()
            .expect("load rotated Store keyring")
            .expect("scoped Store has an established keyring"),
    );
    let mut peer_write = if let Some((database, device)) = &peer {
        device
            .adopt_key_rotation(&rotated_store_encryption, &custody)
            .expect("adopt the key rotation on the concurrent owner device");
        let pulled = device
            .pull_store()
            .await
            .expect("pull the removal on the owner peer")
            .1;
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        database
            .run_scoped_host_write_for_test(
                "INSERT INTO notes (id, audience, body, _updated_at) VALUES (
                '01890a5d-ac96-774b-bcce-b302099c3f77', NULL,
                'captured during removal', '0000000003500-0000-peer');"
                    .to_string(),
            )
            .await;
        let mut writer = device
            .authorize_writer()
            .await
            .expect("authorize the concurrent owner");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("capture the removed-interval write"));
        let captured = StoreDatabase::new(database)
            .oldest_prepared_store_write()
            .await
            .expect("read the captured write")
            .expect("a write is prepared");
        Some((writer, captured.commit.value.reference().clone()))
    } else {
        None
    };

    fixture
        .store
        .admit_member(
            &fixture.owner_database,
            fixture.owner_database_store_dir.clone(),
            &fixture.owner,
            &coven_keys::keys::public_key_hex(&fixture.member),
            None,
            coven_protocol::membership::MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            "Effective Access Store",
        )
        .await
        .expect("re-add effective-access Store member");
    fixture
        .member_device
        .adopt_key_rotation(&rotated_store_encryption, &custody)
        .expect("adopt the Store key wrapped by the re-add");
    let owner_store = fixture
        .store
        .bind_device(
            &fixture.owner_database,
            fixture.owner_database_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("load owner Store for Circle successor");
    owner_store
        .rename_circle(
            "0000000004000-0000-owner",
            fixture.circle_id,
            "Effective Access Restored",
        )
        .await
        .expect("publish Circle successor after Store re-add");
    fixture
        .publish_row(
            READD_EFFECTIVE_ACCESS_ROW_ID,
            "visible after re-add",
            "0000000005000-0000-owner",
        )
        .await;

    if let Some((writer, captured)) = &mut peer_write {
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish the captured write after re-admission"),
            1
        );
        let (_, device) = peer
            .as_ref()
            .expect("the captured write has an owner device");
        assert_eq!(
            device
                .latest_local_store_position()
                .await
                .expect("read the peer publication"),
            Some(captured.clone())
        );
    }

    fixture.home.clear_exact_reads();
    let readd_pull = fixture
        .pull_member()
        .await
        .expect("pull Store re-add and Circle successor");
    assert!(readd_pull.held_positions.is_empty(), "{readd_pull:?}");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(READD_EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible after re-add")
    );
    if publish_removed_interval_write {
        assert_eq!(
            member_database
                .scoped_routing_state_for_test("01890a5d-ac96-774b-bcce-b302099c3f77",)
                .await
                .row
                .as_ref()
                .map(|row| row.1.as_str()),
            Some("captured during removal")
        );
    }
    assert_eq!(
        StoreDatabase::new(&member_database)
            .get_circles(
                &coven_keys::keys::public_key_hex(&fixture.member),
                std::collections::BTreeSet::from([
                    coven_keys::keys::public_key_hex(&fixture.owner),
                    coven_keys::keys::public_key_hex(&fixture.member),
                ]),
            )
            .await
            .expect("list restored Circles")
            .into_iter()
            .map(|circle| circle.name().expect("listed Circle is active").to_string())
            .collect::<Vec<_>>(),
        vec!["Effective Access Restored".to_string()]
    );
}
