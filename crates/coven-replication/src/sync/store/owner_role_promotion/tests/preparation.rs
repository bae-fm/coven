use super::*;

#[tokio::test]
async fn finalization_captures_peer_membership_and_store_predecessor_together() {
    let (fixture, storage) =
        PromotionCandidate::build_with_connection("promotion-peer-membership-predecessor").await;
    let peer_identity = UserKeypair::generate();
    fixture
        .store
        .admit_member(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
            &keys::public_key_hex(&peer_identity),
            None,
            coven_protocol::membership::MemberRole::Member,
            &fixture.encryption,
            "Independent Owner",
        )
        .await
        .expect("admit independent Owner principal");
    let peer_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
    let peer = fixture
        .store
        .activate_joined_device(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &peer_db,
            peer_dir.clone(),
            &peer_identity,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate another Owner device");
    fixture
        .store
        .promote_active_member_fixture(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &peer_db,
            peer_dir,
            &fixture.owner,
            &peer_identity,
            &fixture.encryption,
        )
        .await
        .expect("give the peer an independent Owner authority stream");
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind promotion target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let before = database
        .store_current_publication()
        .await
        .expect("promoter's captured history");
    let invited = keys::public_key_hex(&UserKeypair::generate());
    peer.admit_member(
        &invited,
        None,
        coven_protocol::membership::MemberRole::Member,
        &fixture.encryption,
        &fixture.store.root().store_root_id.to_string(),
        "Peer admission",
    )
    .await
    .expect("peer changes accepted membership before finalization");
    let peer_membership = peer
        .membership_for_test()
        .await
        .expect("peer's accepted membership");
    assert!(peer_membership.can_write_now(&invited));
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unpulled promoter"),
        before,
        "the fixture must leave the promoter's local Store cut behind peer membership"
    );
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect("capture finalization against one accepted membership and Store cut");
    let completed = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("finalized journal")
        .expect("durable completion");
    let OwnerPromotionJournalState::Finalized { receipt, .. } = completed.state else {
        panic!("promotion must complete");
    };
    for head in peer_membership.head_refs() {
        assert!(
            receipt
                .candidate
                .commit
                .membership_state
                .heads
                .contains(head),
            "the candidate must retain the exact peer authority its entry observed"
        );
    }
    let membership = owner
        .membership_for_test()
        .await
        .expect("accepted membership");
    assert!(membership.can_write_now(&invited));
    assert!(membership.is_owner_now(&keys::public_key_hex(&fixture.member)));
    let cold_membership = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &fixture.store.root())
        .await
        .expect("open independent rooted authority reader")
        .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner)))
        .await
        .expect("discover peer dependencies from accepted authority heads");
    assert_eq!(cold_membership.head_refs(), membership.head_refs());
    assert!(cold_membership.can_write_now(&invited));
    assert!(cold_membership.is_owner_now(&keys::public_key_hex(&fixture.member)));
}

#[tokio::test]
async fn finalization_retains_its_author_turn_until_the_candidate_is_durable() {
    use futures_util::FutureExt;

    let fixture = PromotionCandidate::build("promotion-durable-author-turn").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let (prepared, resume) =
        database.arm_test_pause(coven_database::DatabaseTestPoint::OwnerPromotionCandidatePrepared);
    let finalization = owner.finalize_owner_promotion(&fixture.encryption, acceptance.clone());
    tokio::pin!(finalization);
    tokio::select! {
        _ = prepared.notified() => {},
        result = &mut finalization => panic!("finalization ended before candidate staging: {result:?}"),
    }
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("read unstaged candidate")
        .expect("journal exists");
    assert!(matches!(
        journal.state,
        OwnerPromotionJournalState::AcceptanceReady { .. }
    ));
    assert!(database
        .active_store_publication()
        .await
        .expect("read publication reservation")
        .is_none());
    assert!(
        database.author_own_stream().now_or_never().is_none(),
        "another local author must not take the captured position before its candidate is durable"
    );
    resume.notify_one();
    finalization
        .await
        .expect("stage and publish the exact promotion");
    assert!(
        database.author_own_stream().now_or_never().is_some(),
        "completion releases the author turn"
    );
    assert!(matches!(
        database
            .load_owner_promotion_journal(acceptance.request.promotion_id)
            .await
            .expect("read completed journal")
            .expect("journal remains")
            .state,
        OwnerPromotionJournalState::Finalized { .. }
    ));
}
