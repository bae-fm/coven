use coven_keys::keys::{self, UserKeypair};
use coven_protocol::membership::MemberRole;
use coven_protocol::store_commit::ObjectHash;

#[tokio::test]
async fn a_retained_writer_verifies_control_after_another_writer_advances_its_database() {
    use super::{decode_membership_mutation, MembershipMutationPlan};
    use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
    use coven_database::StoreDatabase;
    use coven_keys::encryption::EncryptionService;

    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let owner = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &db,
        store_dir.clone(),
        "retained-writer-predecessor",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir.clone(), &owner)
        .await
        .unwrap();
    let mut writer = device.authorize_writer().await.unwrap();
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, shared, _updated_at, created_at)
         VALUES ('predecessor', 'Accepted row', 1, '0000000001000-0000-owner', '2026-09-08')",
    )
    .await;
    assert!(device.prepare_pending_store_write().await.unwrap());
    assert_eq!(device.drain_store_writes().await.unwrap(), 1);
    let predecessor = device.latest_local_store_position().await.unwrap().unwrap();

    let plan = writer
        .prepare_plan()
        .await
        .expect("prepare from installed history");
    assert!(plan
        .predecessor_cut()
        .unwrap()
        .0
        .values()
        .any(|tip| tip == &predecessor));
    drop(plan);
    let recipient = keys::public_key_hex(&UserKeypair::generate());
    let encryption = EncryptionService::from_key([42; 32]);
    home.fail_exact_create_before_call(1);
    let error = store
        .admit_member(
            &db,
            store_dir.clone(),
            &owner,
            &recipient,
            None,
            MemberRole::Member,
            &encryption,
            "Retained writer",
        )
        .await
        .expect_err("retain a real admission before its first authority upload");
    assert!(
        error
            .to_string()
            .contains("forced failure before exact create call 1"),
        "{error}"
    );
    let database = StoreDatabase::new(&db);
    let row = database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .expect("actual staged admission");
    let (MembershipMutationPlan::Admission(admission), _) =
        decode_membership_mutation(row).unwrap()
    else {
        panic!("the actual request must be an admission");
    };
    let candidate = &admission.candidate;
    assert!(candidate
        .commit
        .order
        .predecessor_cut()
        .unwrap()
        .0
        .values()
        .any(|tip| tip == &predecessor));
    let remotes = candidate
        .merge_membership_activation_remote_objects(std::slice::from_ref(&admission.wrapped_key))
        .unwrap();
    writer
        .publish_membership_authority(candidate, &remotes)
        .await
        .unwrap();
    writer
        .upload_prepared(candidate.clone())
        .await
        .expect("the retained writer verifies control against the actual accepted predecessor");
    drop(writer);
    store
        .admit_member(
            &db,
            store_dir,
            &owner,
            &recipient,
            None,
            MemberRole::Member,
            &encryption,
            "Retained writer",
        )
        .await
        .expect("finish the same staged admission");
    let accepted = database.store_publication_entries().await.unwrap();
    assert_eq!(
        accepted
            .iter()
            .filter(|entry| entry.value.payload
                == coven_protocol::store_commit::StorePublicationPayload::Commit(
                    candidate.reference.clone()
                ))
            .count(),
        1
    );
    assert!(database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert!(database.active_store_publication().await.unwrap().is_none());
}

#[tokio::test]
async fn prepared_membership_transition_rejects_substituted_slots_and_bytes() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        db_store_dir.clone(),
        "prepared-membership-binding",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Merge Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &owner)
        .await
        .expect("bind membership publication Store");
    let chain = device
        .membership_for_test()
        .await
        .expect("load exact membership chain");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize membership publication writer");
    let stream_id = writer
        .select_membership_author_stream(&chain)
        .await
        .expect("select membership stream");
    let entry = chain
        .signed_set_member_in_stream(
            &owner,
            stream_id,
            keys::public_key_hex(&UserKeypair::generate()),
            None,
            MemberRole::Member,
            "2026-07-21T00:00:00Z".to_string(),
        )
        .expect("sign membership entry");
    let prepared = writer
        .prepare_membership_transition(&chain, entry)
        .await
        .expect("prepare membership transition");
    prepared.validate().expect("validate prepared transition");

    let mut redirected_head = prepared.clone();
    redirected_head.transition.head_slot = coven_protocol::objects::ObjectSlot::logical(
        "store-v1/tests/redirected-membership-head.json".to_string(),
    )
    .expect("valid redirected head slot");
    assert!(redirected_head.validate().is_err());

    let mut redirected_successor = prepared.clone();
    redirected_successor.transition.body.successor.next_slot =
        coven_protocol::objects::ObjectSlot::logical(
            "store-v1/tests/redirected-membership-successor.json".to_string(),
        )
        .expect("valid redirected successor slot");
    assert!(redirected_successor.validate().is_err());

    let mut substituted_entry = prepared.clone();
    let substituted_bytes = b"substituted exact membership entry".to_vec();
    let substituted_ref = coven_protocol::objects::ExactObjectRef::new(
        substituted_entry.entry_ref.object.slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_entry.entry_ref.object = substituted_ref.clone();
    substituted_entry.transition.body.entry.object = substituted_ref;
    assert!(substituted_entry.validate().is_err());

    let plan = writer
        .prepare_plan()
        .await
        .expect("prepare membership Store commit");
    let candidate = writer.prepare_candidate(
        &plan,
        crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::MergeMembershipActivation {
            transition: prepared.transition.clone(), stream_activations: Vec::new(),
        },
    ).await.expect("prepare exact activation candidate");
    let mut substituted_head = writer
        .finish_store_membership_transition(prepared, candidate.reference.clone())
        .await
        .expect("finish membership transition");
    let substituted_bytes = b"substituted exact membership head".to_vec();
    let substituted_ref = coven_protocol::objects::ExactObjectRef::new(
        substituted_head.head_ref.object.slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_head.head_ref.object = substituted_ref;
    assert!(substituted_head.validate().is_err());
}

#[tokio::test]
async fn accepted_device_controls_refresh_the_same_writer_membership() {
    use crate::sync::store::device_exclusion::StoreDeviceExclusionResult;
    use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
    use coven_database::StoreDatabase;

    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &owner_db,
        owner_dir.clone(),
        "same-writer-authority",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    store
        .activate_joined_device(
            &owner_db,
            owner_dir.clone(),
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-18T00:00:00Z",
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&owner_db, owner_dir, &signer)
        .await
        .expect("bind owner");
    let database = StoreDatabase::new(&owner_db);
    let target = database
        .activated_store_device_registration_records()
        .await
        .expect("registrations")
        .into_iter()
        .map(|record| record.reference().clone())
        .find(|reference| reference.device_id.to_string() != owner.device_id().as_str())
        .expect("peer registration");
    let mut writer = owner.authorize_writer().await.expect("retain writer");
    let before = writer.membership.head_refs().to_vec();
    let StoreDeviceExclusionResult::ProposalActivated { proposal, commit } = writer
        .device_exclusion()
        .propose(&target)
        .await
        .expect("publish proposal")
    else {
        panic!("proposal must activate");
    };
    let proposal_head = completed_device_control_head(&database, &commit).await;
    assert!(
        !before.contains(&proposal_head),
        "proposal advances accepted authority"
    );
    assert!(
        writer.membership.head_refs().contains(&proposal_head),
        "the same writer must retain its accepted authority head"
    );
    let StoreDeviceExclusionResult::OutcomeActivated { commit, .. } = writer
        .device_exclusion()
        .cancel(&proposal)
        .await
        .expect("publish cancellation with the same writer")
    else {
        panic!("cancellation must activate");
    };
    let outcome_head = completed_device_control_head(&database, &commit).await;
    assert_ne!(outcome_head, proposal_head);
    assert!(writer.membership.head_refs().contains(&outcome_head));
    assert!(database
        .active_outbound_store_device_exclusion()
        .await
        .expect("completed device journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("completed publication")
        .is_none());
}

async fn completed_device_control_head(
    database: &coven_database::StoreDatabase,
    commit: &coven_protocol::store_commit::StoreBatchCommitRef,
) -> coven_protocol::membership::MembershipHeadRef {
    let entries = database
        .store_publication_entries()
        .await
        .expect("installed accepted entries");
    assert_eq!(entries.iter().filter(|entry| matches!(
        &entry.value.payload,
        coven_protocol::store_commit::StorePublicationPayload::Commit(reference) if reference == commit,
    )).count(), 1, "control has one exact accepted publication");
    let operations = database
        .outbound_store_device_exclusion_operations()
        .await
        .expect("device journals");
    let operation = operations
        .iter()
        .find(|operation| {
            operation
                .candidate()
                .is_some_and(|candidate| &candidate.reference == commit)
        })
        .expect("journal for the accepted control");
    assert!(operation.is_completed());
    operation
        .candidate()
        .expect("completed activation retains its candidate")
        .prepared_membership_publication()
        .expect("exact accepted authority graph")
        .head_ref
}
