use crate::database::StoreDatabase;
use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::protocol::membership::MembershipGrantId;
use crate::protocol::objects::{ExactObjectRef, PreparedExactObject};
use crate::protocol::store_commit::ObjectHash;

use super::authority::target_key;
use super::journal::OwnerPromotionJournalState;

#[tokio::test]
async fn second_merge_owner_promotion_verifies_existing_promotion_history() {
    let founder_db = crate::sync::test_helpers::open_test_db();
    let founder = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        "successive-owner-promotions",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Merge Store");
    let first_owner = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    for member in [&first_owner, &second_owner] {
        store
            .invite_member(
                &founder_db,
                &founder,
                &keys::public_key_hex(member),
                None,
                crate::protocol::membership::MemberRole::Member,
                &encryption,
                "Merge Store",
            )
            .await
            .expect("invite Member identity");
    }

    let first_owner_db = crate::sync::test_helpers::open_test_db();
    let second_owner_db = crate::sync::test_helpers::open_test_db();
    store
        .activate_joined_device(
            &founder_db,
            &first_owner_db,
            &first_owner,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate first Owner device");
    store
        .activate_joined_device(
            &founder_db,
            &second_owner_db,
            &second_owner,
            "2026-07-21T00:01:00Z",
        )
        .await
        .expect("activate second Owner device");
    store
        .promote_active_member_fixture(
            &founder_db,
            &first_owner_db,
            &founder,
            &first_owner,
            &encryption,
        )
        .await
        .expect("promote first Owner");

    let second_device = store
        .bind_device(&second_owner_db, &second_owner)
        .await
        .expect("bind second Owner Store");
    let mut second_writer = second_device
        .authorize_writer()
        .await
        .expect("authorize second Owner writer");
    let pull = second_writer
        .pull(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("pull second Owner through the first promotion");
    assert!(pull.held_positions.is_empty());

    store
        .promote_active_member_fixture(
            &founder_db,
            &second_owner_db,
            &founder,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote second Owner");

    let membership = store
        .bind_device(&founder_db, &founder)
        .await
        .expect("bind founder Store")
        .membership_for_test()
        .await
        .expect("load membership after successive promotions");
    assert!(membership.is_owner_now(&keys::public_key_hex(&first_owner)));
    assert!(membership.is_owner_now(&keys::public_key_hex(&second_owner)));
}

#[tokio::test]
async fn merge_owner_promotion_activates_through_its_store_bound_head_and_persists_exact_receipt() {
    let owner_db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_db,
        "merge-owner-promotion",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Merge Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    store
        .invite_member(
            &owner_db,
            &owner,
            &keys::public_key_hex(&member),
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Merge Store",
        )
        .await
        .expect("invite Member identity");
    let member_db = crate::sync::test_helpers::open_test_db();
    store
        .activate_joined_device(&owner_db, &member_db, &member, "2026-07-20T00:00:00Z")
        .await
        .expect("activate Member device");
    let member_registration = store
        .bind_device(&member_db, &member)
        .await
        .expect("bind Member Store")
        .owner_promotion_target_for_test()
        .await
        .expect("load Member promotion target");

    Box::pin(store.promote_active_member_fixture(
        &owner_db,
        &member_db,
        &owner,
        &member,
        &encryption,
    ))
    .await
    .expect("activate Owner promotion");

    assert!(
        StoreDatabase::new(&member_db)
            .load_owner_promotion_target(target_key(&member_registration).unwrap())
            .await
            .expect("load candidate target index")
            .is_none(),
        "the accepting candidate does not own the initiating Owner's target index"
    );

    let owner_device = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("bind promotion owner Store");
    let membership = owner_device
        .membership_for_test()
        .await
        .expect("load activated membership");
    assert!(membership.is_owner_now(&keys::public_key_hex(&member)));
    let promoted_head = membership
        .head_refs()
        .iter()
        .find(|reference| reference.coord.author_pubkey == keys::public_key_hex(&owner))
        .expect("promoter stream head");
    let opened = owner_device
        .load_membership_head_for_test(promoted_head)
        .await
        .expect("load activated promotion head");
    assert!(matches!(
        opened.activation,
        crate::protocol::membership::MembershipHeadActivation::StoreCommit { .. }
    ));

    let mut journal = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load finalized promotion journal")
        .expect("finalized promotion journal exists");
    let OwnerPromotionJournalState::Finalized {
        membership: state,
        receipt,
        ..
    } = &mut journal.state
    else {
        panic!("promotion journal is finalized with Merge membership")
    };
    let publication = &receipt.publication;
    let exact_head = publication.head_ref.clone();
    let index = state
        .heads
        .binary_search(&exact_head)
        .expect("finalized membership contains the exact published head");
    let mut substituted = exact_head;
    substituted.head_hash = ObjectHash::digest(b"substituted same-coordinate head");
    state.heads[index] = substituted;
    state.heads.sort();
    let encoded = serde_json::to_string(&journal).expect("serialize substituted receipt journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", journal.promotion_id),
            &encoded,
        )
        .await
        .expect("install substituted receipt journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .is_err());
}

#[tokio::test]
async fn journal_load_rejects_substituted_request_or_prepared_commit_bytes() {
    let owner_db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_db,
        "corrupt-owner-promotion-request",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create Merge Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    store
        .invite_member(
            &owner_db,
            &owner,
            &keys::public_key_hex(&member),
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Merge Store",
        )
        .await
        .expect("invite Member identity");
    let member_db = crate::sync::test_helpers::open_test_db();
    store
        .activate_joined_device(&owner_db, &member_db, &member, "2026-07-20T00:00:00Z")
        .await
        .expect("activate Member device");
    let member_registration = store
        .bind_device(&member_db, &member)
        .await
        .expect("bind Member Store")
        .owner_promotion_target_for_test()
        .await
        .expect("load Member promotion target");
    home.fail_exact_create_before_call(1);
    store
        .bind_device(&owner_db, &owner)
        .await
        .expect("load Owner Store")
        .begin_owner_promotion(member_registration.clone())
        .await
        .expect_err("interrupted publication retains RequestPrepared");
    let journal = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load prepared request journal")
        .expect("prepared request journal exists");
    let mut substituted_request = journal.clone();
    let OwnerPromotionJournalState::RequestPrepared { request, .. } =
        &mut substituted_request.state
    else {
        panic!("interrupted request remains RequestPrepared")
    };
    request.member_grant = MembershipGrantId(ObjectHash::digest(b"another exact Member grant"));
    let encoded =
        serde_json::to_string(&substituted_request).expect("serialize corrupt request journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", journal.promotion_id),
            &encoded,
        )
        .await
        .expect("install corrupt id journal");
    owner_db
        .set_protocol_state(&target_key(&journal.target).unwrap(), &encoded)
        .await
        .expect("install corrupt target journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .is_err());

    let mut substituted_bytes = journal;
    let OwnerPromotionJournalState::RequestPrepared { candidate, .. } =
        &mut substituted_bytes.state
    else {
        panic!("interrupted request remains RequestPrepared")
    };
    let bytes = b"another exact prepared object".to_vec();
    let reference = ExactObjectRef::new(
        candidate.prepared.reference().slot().clone(),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
    candidate.prepared = PreparedExactObject::new(reference.clone(), bytes)
        .expect("prepare substituted exact object");
    candidate.reference.object = reference;
    let encoded = serde_json::to_string(&substituted_bytes)
        .expect("serialize substituted prepared bytes journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", substituted_bytes.promotion_id),
            &encoded,
        )
        .await
        .expect("install substituted id journal");
    owner_db
        .set_protocol_state(&target_key(&substituted_bytes.target).unwrap(), &encoded)
        .await
        .expect("install substituted target journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(substituted_bytes.promotion_id)
        .await
        .is_err());
}

/// A promotion finalization composes its Store candidate against this device's
/// next stream position, journals it as `MergeHeadPrepared`, and publishes after
/// — the turn that claimed the position is released in between. A queued host
/// write that drains in that window takes the position, and the journaled
/// candidate is bound to that create-once head slot, so it can never activate
/// there. Publication reads the occupant, verifies it is a real winner, and ends
/// the attempt on that evidence: the journal advances to `Stale` instead of
/// re-publishing a candidate that can never land, and the promoter's next attempt
/// for the same target replaces the failed one and activates.
#[tokio::test]
async fn a_promotion_whose_stream_position_was_taken_goes_stale_and_re_issues() {
    let owner_db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_db,
        "owner-promotion-loses-its-position",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create Merge Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    store
        .invite_member(
            &owner_db,
            &owner,
            &keys::public_key_hex(&member),
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Merge Store",
        )
        .await
        .expect("invite Member identity");
    let member_db = crate::sync::test_helpers::open_test_db();
    store
        .activate_joined_device(&owner_db, &member_db, &member, "2026-07-20T00:00:00Z")
        .await
        .expect("activate Member device");
    let member_registration = store
        .bind_device(&member_db, &member)
        .await
        .expect("bind Member Store")
        .owner_promotion_target_for_test()
        .await
        .expect("load Member promotion target");

    let request = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("load Owner Store")
        .begin_owner_promotion(member_registration.clone())
        .await
        .expect("publish the promotion request");
    let acceptance = store
        .bind_device(&member_db, &member)
        .await
        .expect("load Member Store")
        .accept_owner_promotion(request)
        .await
        .expect("accept the promotion");

    // A queued host write composes against the same next position the
    // finalization will, and takes it the moment it drains.
    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('contended-note', 'contended', NULL, 1, \
                 '0000000001000-0000-owner', '2026-07-20')",
        )
        .await;
    let loaded_owner_store = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("load promoter Store");
    let mut writer = loaded_owner_store
        .authorize_writer()
        .await
        .expect("authorize promoter writer");
    assert!(Box::pin(writer.prepare_pending_store_write())
        .await
        .expect("queue a host write at the contended position"));

    // Stop the finalization after it journals its composed candidate and before
    // it publishes the head that would take the position.
    home.fail_exact_create_before_call(5);
    Box::pin(
        store
            .bind_device(&owner_db, &owner)
            .await
            .expect("load Owner Store")
            .finalize_owner_promotion(&encryption, acceptance.clone()),
    )
    .await
    .expect_err("the interrupted finalization cannot publish its head");
    let interrupted = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load the interrupted promotion journal")
        .expect("the interrupted promotion journal exists");
    assert!(
        matches!(
            interrupted.state,
            OwnerPromotionJournalState::MergeHeadPrepared { .. }
        ),
        "the interruption leaves a composed candidate bound to its position: {:?}",
        interrupted.state,
    );

    assert_eq!(
        Box::pin(writer.drain_store_writes())
            .await
            .expect("publish the queued host write"),
        1,
    );

    let lost = Box::pin(
        store
            .bind_device(&owner_db, &owner)
            .await
            .expect("load Owner Store")
            .finalize_owner_promotion(&encryption, acceptance),
    )
    .await
    .expect_err("a candidate whose position was taken can never activate");
    assert!(
        matches!(
            &lost,
            crate::sync::store::owner::owner_promotion::OwnerPromotionError::Stale(reason)
                if matches!(
                    reason.as_ref(),
                    crate::protocol::store_commit::OwnerPromotionStaleReason::MergeActivationRejected
                )
        ),
        "the finalization ends on the verified winner: {lost}",
    );
    let ended = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load the ended promotion journal")
        .expect("the ended promotion journal exists");
    assert!(
        matches!(ended.state, OwnerPromotionJournalState::Stale { .. }),
        "the lost attempt is recorded stale rather than re-published: {:?}",
        ended.state,
    );

    Box::pin(store.promote_active_member_fixture(
        &owner_db,
        &member_db,
        &owner,
        &member,
        &encryption,
    ))
    .await
    .expect("a fresh attempt replaces the stale one and activates");
    assert!(store
        .bind_device(&owner_db, &owner)
        .await
        .expect("bind re-issued promotion Store")
        .membership_for_test()
        .await
        .expect("load membership after the re-issued promotion")
        .is_owner_now(&keys::public_key_hex(&member)));
}
