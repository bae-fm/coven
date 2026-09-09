use super::*;

#[tokio::test]
async fn membership_preparation_waits_for_its_predecessors_accepted_result() {
    let fixture = MergeFixture::new("membership-predecessor-result").await;
    let peer_directory = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_directory.clone());
    let peer = fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &peer_db,
            peer_directory.clone(),
            &fixture.owner,
            "2026-09-09T00:00:00Z",
        )
        .await
        .expect("activate another device on the Owner membership stream");
    let predecessor = fixture.load().await;
    let tip = predecessor.head_refs().last().expect("Owner head");
    let prior_head = fixture
        .device
        .load_membership_head_for_test(tip)
        .await
        .expect("load predecessor head");
    let first = pubkey_hex(&UserKeypair::generate());
    let second = pubkey_hex(&UserKeypair::generate());
    let encryption = EncryptionService::from_key([42; 32]);
    let (accepted, release) = fixture.home.pause_next_conditional_replace();
    let mut admission = Box::pin(fixture.store.admit_member(
        &fixture.db,
        fixture.store_dir.clone(),
        &fixture.owner,
        &first,
        None,
        MemberRole::Member,
        &encryption,
        "Test Store",
    ));
    tokio::select! {
        _ = accepted.notified() => {},
        result = &mut admission => panic!("admission ended before accepted publication: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("admission never reached acceptance"),
    }
    fixture.home.fail_exact_create_before_call(1);
    release.notify_one();
    let failure = admission
        .await
        .expect_err("interrupt accepted result upload");
    assert!(
        failure
            .to_string()
            .contains("forced failure before exact create call 1"),
        "{failure}"
    );
    let bytes = fixture
        .home
        .stored_exact_bytes(&prior_head.body.successor.next_slot)
        .expect("the accepted control's membership head was uploaded");
    let head: AuthorHead = coven_protocol::objects::decode_protocol_object(&bytes)
        .expect("decode accepted membership head");
    let coven_protocol::membership::MembershipHeadActivation::StoreCommit {
        acceptance_slot,
        commit,
    } = &head.activation
    else {
        panic!("the control requires its Store acceptance result");
    };
    let active = fixture
        .database
        .active_store_publication()
        .await
        .expect("read accepted pending publication")
        .expect("origin retains finalization");
    assert_eq!(
        active.commit_reservation().expect("reserved commit").2,
        &commit.coord
    );
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_none());

    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer installs the accepted control");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('while-finalizing', 'peer edit', 1, '0000000001000-0000-peer', '2026-09-09')",
        )
        .await;
    assert!(peer
        .publish_pending_store_database()
        .await
        .expect("ordinary peer writes remain eligible"));
    let peer_database = StoreDatabase::new(&peer_db);
    let rejected = fixture
        .store
        .admit_member(
            &peer_db,
            peer_directory.clone(),
            &fixture.owner,
            &second,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect_err("a successor cannot be prepared before its predecessor result exists");
    assert!(rejected.to_string().contains("finalization"), "{rejected}");
    assert!(peer_database
        .outbound_membership_mutation()
        .await
        .expect("peer journal")
        .is_none());
    assert!(peer_database
        .active_store_publication()
        .await
        .expect("peer reservation")
        .is_none());
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_none());

    fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.owner,
            &first,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("origin finalizes its exact accepted control");
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_some());
    let resumed = fixture
        .store
        .admit_member(
            &peer_db,
            peer_directory,
            &fixture.owner,
            &second,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("prepare and publish after predecessor finalization");
    assert!(resumed
        .membership_floor
        .0
        .iter()
        .any(
            |reference| reference.coord.stream_key() == head.entry_coord().stream_key()
                && reference.coord.seq == head.entry_coord().seq + 1
        ));
}

#[tokio::test]
async fn a_rollup_cannot_substitute_another_signed_predecessor_result() {
    let fixture = MergeFixture::new("rollup-predecessor-result").await;
    fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    let admission = fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    let snapshot = fixture
        .device
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish accepted membership rollup");
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipRollup,
    );
    let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
        &snapshot.meta.membership_rollup.object,
        ".json",
    )
    .expect("rollup prefix");
    let bytes = fixture
        .storage
        .read_protocol_object(&context, &snapshot.meta.membership_rollup.object, &prefix)
        .await
        .expect("read actual published rollup");
    let mut rollup: coven_protocol::store_commit::MembershipRollup =
        coven_protocol::objects::decode_protocol_object(&bytes).expect("decode rollup");
    let registration = fixture
        .database
        .activated_store_device_registration(rollup.author_registration.clone())
        .await
        .expect("actual publisher registration");
    let signer = registration
        .value()
        .device_signer(&fixture.owner)
        .expect("actual device signer");
    let stream = rollup
        .body_mut()
        .streams
        .iter_mut()
        .find(|stream| stream.author_pubkey == fixture.owner_pubkey)
        .expect("Owner stream");
    let previous_head = stream.heads[stream.heads.len() - 2].head_value.clone();
    let last = stream.heads.last_mut().expect("terminal covered head");
    let result = last
        .predecessor_acceptance
        .as_mut()
        .expect("accepted predecessor result");
    assert!(!result.accepted_predecessor.0.is_empty());
    let selected_head = result.head.clone();
    result.body_mut().accepted_predecessor =
        coven_protocol::membership::MembershipFloor(vec![selected_head]);
    result.resign(&signer);
    result
        .verify_for(
            fixture.store.root().store_root_hash,
            &result.head,
            &previous_head,
            registration.value(),
        )
        .expect("alternate result still has a valid issuer signature and exact head");
    let coven_protocol::membership::MembershipHeadPredecessor::Accepted { acceptance, .. } = last
        .head_value
        .body_mut()
        .body
        .predecessor
        .as_mut()
        .expect("predecessor")
    else {
        panic!("Store predecessor needs its accepted result");
    };
    assert!(acceptance.verify(&result.to_bytes()).is_err());
    let replacement = result.to_bytes();
    *acceptance = ExactObjectRef::new(
        acceptance.slot().clone(),
        replacement.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&replacement),
    );
    last.head_value.resign(&signer);
    last.head.head_hash = last.head_value.head_hash();
    let replacement_head = last.head_value.to_bytes();
    last.head.object = ExactObjectRef::new(
        last.head.object.slot().clone(),
        replacement_head.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&replacement_head),
    );
    rollup.resign(&signer);
    rollup
        .validate_shape()
        .expect("alternate rollup has internally valid exact objects");

    let expected = walk_admission_membership(&fixture, &admission, false).await;
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*fixture.storage, &admission.store_root)
        .await
        .expect("independent cold reader");
    history
        .admit_membership_rollup(&rollup)
        .await
        .expect("admit signed advisory objects");
    let actual = history
        .load_accepted_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("provider-pinned successor selects its genuine predecessor result");
    assert_eq!(actual.head_refs(), expected.head_refs());
    assert_eq!(actual.effective_frontier(), expected.effective_frontier());
}
