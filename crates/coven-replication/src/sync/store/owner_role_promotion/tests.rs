use coven_database::StoreDatabase;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::membership::MembershipGrantId;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::store_commit::ObjectHash;

use super::journal::target_key;
use super::journal::OwnerPromotionJournalState;

mod admission;
mod excluded_authority;
mod finalization;
mod issuer_retirement;
mod preparation;
mod recovery;
mod request_retirement;
mod rotation_staging;
mod snapshot_authority;

/// A Merge Store with one activated Member device and the promotion target that
/// device's registration names — the starting point of every promotion case that
/// works on a single candidate.
struct PromotionCandidate {
    owner_db: coven_database::Database,
    owner_db_store_dir: coven_foundation::store_dir::StoreDir,
    owner: UserKeypair,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    member: UserKeypair,
    member_db: coven_database::Database,
    member_db_store_dir: coven_foundation::store_dir::StoreDir,
    member_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
    encryption: EncryptionService,
}

impl PromotionCandidate {
    async fn build(store_name: &str) -> Self {
        Self::build_with_connection(store_name).await.0
    }

    async fn build_with_connection(
        store_name: &str,
    ) -> (Self, std::sync::Arc<coven_storage::CloudSyncConnection>) {
        let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
        let owner = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = crate::sync::test_helpers::TestStore::create_with_connection(
            &owner_db,
            owner_db_store_dir.clone(),
            store_name,
            owner.clone(),
            home.clone(),
        )
        .await
        .expect("create Merge Store");
        let member = UserKeypair::generate();
        let encryption = EncryptionService::from_key([42; 32]);
        store
            .admit_member(
                &owner_db,
                owner_db_store_dir.clone(),
                &owner,
                &keys::public_key_hex(&member),
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Merge Store",
            )
            .await
            .expect("admit Member identity");
        let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
        store
            .activate_joined_device(
                &owner_db,
                owner_db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &member,
                "2026-07-20T00:00:00Z",
            )
            .await
            .expect("activate Member device");
        let member_registration = store
            .bind_device_in(&member_db, member_db_store_dir.clone(), &member)
            .await
            .expect("bind Member Store")
            .owner_promotion_target_for_test()
            .await
            .expect("load Member promotion target");
        (
            Self {
                owner_db,
                owner_db_store_dir,
                owner,
                home,
                store,
                member,
                member_db,
                member_db_store_dir,
                member_registration,
                encryption,
            },
            storage,
        )
    }
}

#[tokio::test]
async fn interrupted_promotion_request_retains_its_candidate_objects() {
    let fixture = PromotionCandidate::build("retained-promotion-request").await;
    fixture.home.fail_exact_create_before_call(1);
    fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind Owner Store")
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect_err("interrupt before candidate upload");
    let journal = StoreDatabase::new(&fixture.owner_db)
        .load_owner_promotion_target(target_key(&fixture.member_registration).unwrap())
        .await
        .expect("load promotion journal")
        .expect("promotion remains durable");
    let OwnerPromotionJournalState::RequestPrepared { candidate, .. } = journal.state else {
        panic!("interrupted promotion must retain its prepared request");
    };
    let object = &candidate.reference.object;
    let retained = fixture
        .owner_db
        .remote_object_for_test(object.clone())
        .await
        .expect("candidate object ownership precedes upload");
    assert_eq!(retained.object(), object);
    assert!(!retained.records_verified_upload());

    let database = StoreDatabase::new(&fixture.owner_db);
    let active = database
        .active_store_publication()
        .await
        .expect("read interrupted publication")
        .expect("request owns an active attempt");
    assert_eq!(
        active.attempt().expect("prepared publication"),
        &candidate.publication
    );
    assert!(!fixture
        .owner_db
        .remote_object_exists_for_test(
            active
                .attempt()
                .expect("prepared publication")
                .entry_object
                .clone()
        )
        .await
        .expect("inspect publication ownership"));
    fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("reopen Owner Store after interrupted publication")
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("resume the retained promotion request");
    let object = &candidate.reference.object;
    let retained = fixture
        .owner_db
        .remote_object_for_test(object.clone())
        .await
        .expect("accepted candidate retains exact ownership");
    assert!(retained.records_verified_upload());
    assert_eq!(retained.object(), object);

    assert!(database
        .active_store_publication()
        .await
        .expect("read completed attempt")
        .is_none());
    let accepted = database
        .store_publication_entries()
        .await
        .expect("read accepted publication entries")
        .into_iter()
        .find(|entry| {
            entry.value.payload
                == coven_protocol::store_commit::StorePublicationPayload::Commit(
                    candidate.reference.clone(),
                )
        })
        .expect("request is accepted under its exact commit reference");
    assert_eq!(
        accepted.value,
        active.attempt().expect("prepared publication").entry
    );
    assert_eq!(
        accepted.prepared.reference(),
        &active.attempt().expect("prepared publication").entry_object
    );
}

#[tokio::test]
async fn second_merge_owner_promotion_verifies_existing_promotion_history() {
    let founder_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = crate::sync::test_helpers::open_test_db(founder_db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        founder_db_store_dir.clone(),
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
            .admit_member(
                &founder_db,
                founder_db_store_dir.clone(),
                &founder,
                &keys::public_key_hex(member),
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Merge Store",
            )
            .await
            .expect("admit Member identity");
    }

    let first_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let first_owner_db = crate::sync::test_helpers::open_test_db(first_owner_db_store_dir.clone());
    let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_db =
        crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
    store
        .activate_joined_device(
            &founder_db,
            founder_db_store_dir.clone(),
            &first_owner_db,
            first_owner_db_store_dir.clone(),
            &first_owner,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate first Owner device");
    store
        .activate_joined_device(
            &founder_db,
            founder_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            "2026-07-21T00:01:00Z",
        )
        .await
        .expect("activate second Owner device");
    store
        .promote_active_member_fixture(
            &founder_db,
            founder_db_store_dir.clone(),
            &first_owner_db,
            first_owner_db_store_dir.clone(),
            &founder,
            &first_owner,
            &encryption,
        )
        .await
        .expect("promote first Owner");

    let second_device = store
        .bind_device_in(
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
        )
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
            founder_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote second Owner");

    let membership = store
        .bind_device_in(&founder_db, founder_db_store_dir.clone(), &founder)
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
    let PromotionCandidate {
        owner_db,
        owner_db_store_dir,
        owner,
        home: _home,
        store,
        member,
        member_db,
        member_db_store_dir,
        member_registration,
        encryption,
    } = PromotionCandidate::build("merge-owner-promotion").await;

    Box::pin(store.promote_active_member_fixture(
        &owner_db,
        owner_db_store_dir.clone(),
        &member_db,
        member_db_store_dir.clone(),
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
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
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
        coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. }
    ));

    let mut journal = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load finalized promotion journal")
        .expect("finalized promotion journal exists");
    let OwnerPromotionJournalState::Finalized {
        membership: state,
        candidate,
        ..
    } = &mut journal.state
    else {
        panic!("promotion journal is finalized with Merge membership")
    };
    let publication = candidate
        .prepared_membership_publication()
        .expect("finalized publication");
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
    let PromotionCandidate {
        owner_db,
        owner_db_store_dir,
        owner,
        home,
        store,
        member: _member,
        member_db: _member_db,
        member_db_store_dir: _member_db_store_dir,
        member_registration,
        encryption: _encryption,
    } = PromotionCandidate::build("corrupt-owner-promotion-request").await;
    home.fail_exact_create_before_call(1);
    store
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
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
    request.body_mut().member_grant =
        MembershipGrantId(ObjectHash::digest(b"another exact Member grant"));
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
        candidate.reference.object.slot().clone(),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
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

#[tokio::test]
async fn promotion_waits_for_the_reserved_host_write_and_resumes_its_same_attempt() {
    let PromotionCandidate {
        owner_db,
        owner_db_store_dir,
        owner,
        home: _home,
        store,
        member,
        member_db,
        member_db_store_dir,
        member_registration,
        encryption,
    } = PromotionCandidate::build("owner-promotion-reserved-position").await;

    let request = store
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("load Owner Store")
        .begin_owner_promotion(member_registration.clone())
        .await
        .expect("publish the promotion request");
    let acceptance = store
        .bind_device_in(&member_db, member_db_store_dir.clone(), &member)
        .await
        .expect("load Member Store")
        .accept_owner_promotion(request)
        .await
        .expect("accept the promotion");

    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('contended-note', 'contended', NULL, 1, \
                 '0000000001000-0000-owner', '2026-07-20')",
        )
        .await;
    let loaded_owner_store = store
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("load promoter Store");
    let mut writer = loaded_owner_store
        .authorize_writer()
        .await
        .expect("authorize promoter writer");
    assert!(Box::pin(writer.prepare_pending_store_write())
        .await
        .expect("queue a host write at the contended position"));

    let reserved = StoreDatabase::new(&owner_db)
        .active_store_publication()
        .await
        .expect("read reserved host publication")
        .expect("prepared host write reserves its publication");
    Box::pin(
        store
            .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
            .await
            .expect("load Owner Store")
            .finalize_owner_promotion(&encryption, acceptance.clone()),
    )
    .await
    .expect_err("promotion cannot take a reserved host publication");
    let interrupted = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load the interrupted promotion journal")
        .expect("the interrupted promotion journal exists");
    assert!(
        matches!(
            interrupted.state,
            OwnerPromotionJournalState::AcceptanceReady { .. }
        ),
        "promotion retains its accepted request before reserving a candidate: {:?}",
        interrupted.state,
    );

    assert_eq!(
        StoreDatabase::new(&owner_db)
            .active_store_publication()
            .await
            .expect("read reservation after rejected promotion"),
        Some(reserved),
    );
    assert!(!store
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("reopen owner before publication")
        .membership_for_test()
        .await
        .expect("read membership before publication")
        .is_owner_now(&keys::public_key_hex(&member)));

    assert_eq!(
        Box::pin(writer.drain_store_writes())
            .await
            .expect("publish the queued host write"),
        1,
    );

    Box::pin(
        store
            .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
            .await
            .expect("reopen promoter after host publication")
            .finalize_owner_promotion(&encryption, acceptance),
    )
    .await
    .expect("resume the same promotion after its predecessor publishes");
    let finalized = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load finalized promotion")
        .expect("promotion journal remains durable");
    assert_eq!(finalized.promotion_id, interrupted.promotion_id);
    assert!(matches!(
        finalized.state,
        OwnerPromotionJournalState::Finalized { .. }
    ));
    assert!(store
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("reopen promoted Store")
        .membership_for_test()
        .await
        .expect("read accepted promotion")
        .is_owner_now(&keys::public_key_hex(&member)));
}

#[tokio::test]
async fn a_prepared_promotion_cannot_replace_its_exact_membership_head() {
    let fixture = PromotionCandidate::build("promotion-exact-head-identity").await;
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
        .expect("publish promotion request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept promotion request");
    fixture.home.fail_exact_create_before_call(1);
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect_err("retain the prepared promotion before upload");
    let database = StoreDatabase::new(&fixture.owner_db);
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("load prepared promotion")
        .expect("promotion journal remains owned");
    let OwnerPromotionJournalState::MergeHeadPrepared {
        candidate: original_candidate,
        ..
    } = &journal.state
    else {
        panic!("promotion must retain its prepared membership head");
    };
    let original_publication = original_candidate
        .prepared_membership_publication()
        .expect("original candidate owns its exact membership publication");
    let registration = database
        .activated_store_device_registration(original_candidate.commit.author_registration.clone())
        .await
        .expect("load the actual promoter registration");
    let signer = registration
        .value()
        .device_signer(&fixture.owner)
        .expect("recover the actual promoter device signer");
    let mut replacement = journal.clone();
    let OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } = &mut replacement.state
    else {
        panic!("the copied journal must retain the same state");
    };
    let proof = candidate
        .history_evidence
        .membership_proof
        .as_mut()
        .expect("promotion candidate retains its signed membership proof");
    let coven_protocol::membership::MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &mut proof.head_value.body_mut().activation
    else {
        panic!("promotion head must be activated by its Store candidate");
    };
    // Keep the semantic acceptance position but substitute another physical
    // reservation, which the activating commit does not contain.
    *acceptance_slot = coven_protocol::objects::ObjectSlot::opaque(
        acceptance_slot.logical_key().to_string(),
        "substituted-promotion-acceptance-reservation".to_string(),
    )
    .expect("valid alternate acceptance slot");
    proof.head_value.resign(&signer);
    assert!(proof.head_value.verify(registration.value()));
    let head_bytes = proof.head_value.to_bytes();
    proof.head.head_hash = proof.head_value.head_hash();
    proof.head.object = ExactObjectRef::new(
        proof.head.object.slot().clone(),
        head_bytes.len() as u64,
        ObjectHash::digest(&head_bytes),
    );
    let publication = candidate
        .prepared_membership_publication()
        .expect("changed proof still binds one internally valid publication");
    assert_eq!(candidate.reference, original_candidate.reference);
    assert_eq!(
        candidate.commit.to_bytes(),
        original_candidate.commit.to_bytes()
    );
    assert_ne!(publication.head_ref, original_publication.head_ref);
    replacement
        .validate_id(journal.promotion_id)
        .expect("each journal independently contains a valid signed promotion");
    journal
        .validate_transition(&journal)
        .expect("retry may retain the original exact head");
    assert!(matches!(
        journal.validate_transition(&replacement),
        Err(coven_protocol::owner_promotion_journal::OwnerPromotionJournalError::Invariant(_))
    ));
}

#[tokio::test]
async fn promotion_journal_rejects_an_unrelated_circle_acknowledgement() {
    use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
    use coven_protocol::store_commit::*;
    use coven_storage::CloudSyncObjectStorage;

    let (fixture, storage) =
        PromotionCandidate::build_with_connection("promotion-unrelated-circle-ack").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .unwrap();
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .unwrap();
    let circle = owner
        .create_circle("0000000001000-0000-owner", "Promotion observer")
        .await
        .unwrap();
    let frontier = owner.acknowledgement_frontier().await.unwrap();
    owner
        .stage_circle_acknowledgements(&frontier, "2026-07-20T00:00:01Z")
        .await
        .unwrap();
    owner
        .stage_current_acknowledgement_if_new("2026-07-20T00:00:01Z")
        .await
        .unwrap();
    assert_eq!(owner.drain_acknowledgements_exact().await.unwrap(), 1);
    let database = StoreDatabase::new(&fixture.owner_db);
    let acknowledgement = database
        .latest_published_circle_ack(circle)
        .await
        .unwrap()
        .expect("the Circle owner published an actual acknowledgement")
        .reference;
    assert_eq!(
        storage
            .observe_exact_slot(acknowledgement.object.slot())
            .await
            .unwrap(),
        Some(acknowledgement.object.clone())
    );

    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .unwrap();
    let acceptance = member.accept_owner_promotion(request).await.unwrap();
    fixture.home.fail_exact_create_before_call(1);
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect_err("retain the real prepared promotion before upload");
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .unwrap()
        .unwrap();
    journal
        .validate_id(journal.promotion_id)
        .expect("the original promotion journal is valid");
    let mut substituted = journal.clone();
    let OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } = &mut substituted.state
    else {
        panic!("promotion must retain its prepared membership head");
    };
    let mut publication = candidate
        .prepared_membership_publication()
        .expect("the actual retained candidate owns its original membership publication");
    let registration = database
        .activated_store_device_registration(candidate.commit.author_registration.clone())
        .await
        .unwrap();
    let signer = registration.value().device_signer(&fixture.owner).unwrap();
    assert_eq!(
        acknowledgement.registration,
        candidate.commit.author_registration
    );
    let original = candidate.commit.clone();
    let operations = original.operations().unwrap();
    assert!(operations.circle_acknowledgements.is_empty());
    let commit = StoreBatchCommit::signed_operations(
        original.store_root_hash,
        original.write_id.clone(),
        candidate.reference.coord.clone(),
        original.author_registration.clone(),
        registration.value(),
        original.order.clone(),
        original.publication_base.clone(),
        original.membership_state.clone(),
        original.device_state.clone(),
        original.operations_membership_authority().unwrap(),
        StoreCommitOperationsInput {
            control: operations.control.clone(),
            stream_activations: operations.stream_activations.clone(),
            circle_acknowledgements: vec![acknowledgement.clone()],
            ..StoreCommitOperationsInput::empty()
        },
        &signer,
    )
    .expect("the general batch signer permits a membership control with an acknowledgement");
    let context = ProtocolObjectContext::signed_plaintext(
        original.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &candidate.reference.coord.stream_id.to_string(),
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .unwrap();
    let prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .unwrap();
    let verified = VerifiedStoreBatchCommit::parse_prepared(
        &commit.to_bytes(),
        original.store_root_hash,
        candidate.reference.coord.clone(),
        prepared.reference().clone(),
        registration.value(),
    )
    .expect("the altered candidate has a valid signature and exact manifest");
    candidate.common.commit = commit;
    candidate.common.reference = verified.reference().clone();
    assert_eq!(
        candidate.commit.circle_acknowledgements(),
        &[acknowledgement]
    );
    assert_eq!(candidate.commit.control(), original.control());
    assert_eq!(
        candidate.commit.operations().unwrap().stream_activations,
        operations.stream_activations
    );

    // Rebuild the actual publication through its signing owners, so rejection
    // cannot be attributed to a stale hash, signature, or replacement record.
    let entry =
        StorePublicationEntry::signed_commit(&candidate.publication.previous, &verified, &signer)
            .unwrap();
    let context = ProtocolObjectContext::signed_plaintext(
        original.store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .unwrap();
    let prepared_entry = storage
        .prepare_protocol_object(&context, slot, &prefix, entry.to_bytes())
        .unwrap();
    let reference =
        StorePublicationRef::from_entry(&entry, prepared_entry.reference().clone()).unwrap();
    let replacement = StoreCurrentPublicationRecord::advance_commit(
        &candidate.publication.previous,
        &entry,
        reference,
        &verified,
        &signer,
    )
    .unwrap();
    candidate.publication.entry = entry;
    candidate.publication.entry_object = prepared_entry.reference().clone();
    candidate.publication.replacement = replacement;
    candidate
        .publication
        .verify_commit(&verified)
        .expect("the new publication cryptographically binds the altered candidate");

    let coven_protocol::membership::MembershipHeadActivation::StoreCommit { commit, .. } =
        &mut publication.head.body_mut().activation
    else {
        panic!("promotion head must activate its Store candidate");
    };
    *commit = candidate.reference.clone();
    publication.head.resign(&signer);
    assert!(publication.head.verify(registration.value()));
    let head_bytes = publication.head.to_bytes();
    publication.head_ref.head_hash = publication.head.head_hash();
    publication.head_ref.object = ExactObjectRef::new(
        publication.head_ref.object.slot().clone(),
        head_bytes.len() as u64,
        ObjectHash::digest(&head_bytes),
    );
    candidate
        .attach_merge_membership_proof_with(&publication, None)
        .expect("the signed head and retained evidence bind the altered candidate");
    candidate
        .validate_closed_shape()
        .expect("all candidate proof and exact byte identities remain valid");
    candidate
        .prepared_membership_publication()
        .expect("the candidate derives a valid membership publication");

    let encoded = serde_json::to_string(&substituted).unwrap();
    fixture
        .owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", substituted.promotion_id),
            &encoded,
        )
        .await
        .unwrap();
    let error = database
        .load_owner_promotion_journal(substituted.promotion_id)
        .await
        .expect_err("promotion owns exactly its membership control and two stream activations");
    assert!(
        matches!(&error,
            coven_database::DbError::OwnerPromotionJournal(cause)
                if matches!(cause.as_ref(), coven_protocol::owner_promotion_journal::OwnerPromotionJournalError::Invariant(_))
        ),
        "{error}"
    );
}
