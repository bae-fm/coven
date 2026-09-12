use super::*;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;

#[tokio::test]
async fn staging_a_removal_derives_its_rotation_from_the_prepared_request() {
    let fixture = PromotionCandidate::build("removal-staging-generation").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind removal owner");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("install accepted predecessor");
    assert!(pulled.held_positions.is_empty());
    let database = StoreDatabase::new(&fixture.owner_db);
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert!(database.load_rotation_gate().await.unwrap().is_none());
    let before = fixture.owner_db.database_image_for_test().await.unwrap();
    fixture.home.fail_exact_create_before_call(1);
    fixture
        .store
        .remove_member(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
            &keys::public_key_hex(&fixture.member),
            &fixture.encryption,
            &crate::sync::test_helpers::TestCustody::default(),
        )
        .await
        .expect_err("retain real removal before its first upload");
    let row = database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    let gate = database
        .load_rotation_gate()
        .await
        .unwrap()
        .expect("production removal staged its rotation");
    let encoded: serde_json::Value = serde_json::from_slice(&row.plan_bytes).unwrap();
    let candidate: PreparedStoreOperationCommit =
        serde_json::from_value(encoded["plan"]["candidate"].clone()).unwrap();
    let remotes = candidate
        .merge_membership_activation_remote_objects()
        .unwrap();
    drop(owner);
    fixture
        .owner_db
        .replace_with_database_image_for_test(before)
        .await
        .unwrap();
    assert!(database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert!(database.load_rotation_gate().await.unwrap().is_none());
    let staged = database
        .stage_membership_candidate_mutation(row.plan_bytes, row.progress_bytes, remotes, candidate)
        .await;
    assert_eq!(
        staged.expect("stage the exact removal request"),
        row.intent_hash
    );
    assert_eq!(
        database.load_rotation_gate().await.unwrap(),
        Some(gate),
        "the database must derive the rotation from its prepared removal"
    );
}

#[tokio::test]
async fn installed_removal_completion_derives_its_committed_rotation() {
    complete_installed_removal(CompletionProof::Exact).await;
}

#[tokio::test]
async fn installed_removal_completion_refuses_missing_proof_atomically() {
    complete_installed_removal(CompletionProof::Missing).await;
}

#[tokio::test]
async fn installed_removal_completion_refuses_altered_rotation_proof_atomically() {
    complete_installed_removal(CompletionProof::AlteredRotation).await;
}

enum CompletionProof {
    Exact,
    Missing,
    AlteredRotation,
}

async fn complete_installed_removal(proof: CompletionProof) {
    use coven_protocol::membership_mutation::StoreMembershipJournalCompletion;
    use coven_protocol::objects::{LocalRotation, RotationGate};

    let fixture = PromotionCandidate::build("installed-removal-rotation").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .unwrap();
    owner.pull_store().await.unwrap();
    let database = StoreDatabase::new(&fixture.owner_db);
    let member = keys::public_key_hex(&fixture.member);
    let custody = crate::sync::test_helpers::TestCustody::default();
    let (accepted, _release) = fixture.home.pause_next_conditional_replace();
    {
        let removal = fixture.store.remove_member(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
            &member,
            &fixture.encryption,
            &custody,
        );
        tokio::pin!(removal);
        tokio::select! {
            _ = accepted.notified() => {},
            result = &mut removal => panic!("removal ended before accepted publication: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("removal did not reach acceptance"),
        }
    }
    let row = database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    let encoded: serde_json::Value = serde_json::from_slice(&row.plan_bytes).unwrap();
    let candidate: PreparedStoreOperationCommit =
        serde_json::from_value(encoded["plan"]["candidate"].clone()).unwrap();
    let remotes = candidate
        .merge_membership_activation_remote_objects()
        .unwrap();
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("install the accepted removal independently");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let verified = owner
        .load_commit_for_test(&candidate.reference)
        .await
        .unwrap();
    let acceptance = database
        .installed_store_commit_evidence(verified.clone())
        .await
        .unwrap()
        .expect("pull installed the exact removal");
    let completion = || StoreMembershipJournalCompletion::Mutation {
        intent_hash: row.intent_hash,
        progress_bytes: serde_json::to_vec(&serde_json::json!({
            "state": "revoke_activated", "candidate": candidate.reference,
        }))
        .unwrap(),
        remote_objects: remotes
            .iter()
            .map(|remote| remote.record().clone())
            .collect(),
    };
    if !matches!(proof, CompletionProof::Exact) {
        let mut invalid = candidate.history_evidence.clone();
        match proof {
            CompletionProof::Missing => invalid.membership_proof = None,
            CompletionProof::AlteredRotation => {
                let entry = &mut invalid.membership_proof.as_mut().unwrap().entry_value;
                let coven_protocol::membership::StoreAuthorityChange::RemoveMember {
                    rotation_generation,
                    ..
                } = &mut entry.body_mut().change
                else {
                    panic!("the real candidate is a member removal");
                };
                *rotation_generation += 1;
            }
            CompletionProof::Exact => unreachable!("exact proof is completed below"),
        }
        let before = fixture.owner_db.database_image_for_test().await.unwrap();
        let error = database
            .complete_installed_store_operation(
                verified.clone(),
                acceptance.clone(),
                invalid,
                None,
                Some(completion()),
            )
            .await
            .expect_err("installed completion must refuse invalid candidate proof");
        assert!(
            matches!(error, coven_database::DbError::Protocol(ref cause)
            if matches!(**cause, coven_protocol::store_commit::StoreProtocolError::DeviceStateMismatch)),
            "{error}"
        );
        assert_eq!(
            fixture.owner_db.database_image_for_test().await.unwrap(),
            before,
            "proof refusal must preserve journal, rotation, reservation and exact object states"
        );
    }
    database
        .complete_installed_store_operation(
            verified,
            acceptance,
            candidate.history_evidence.clone(),
            None,
            Some(completion()),
        )
        .await
        .expect("complete the retained removal request");
    let gate = database.load_rotation_gate().await.unwrap().unwrap();
    let local = match gate {
        RotationGate::Local(local) | RotationGate::LocalAndPeer { local, .. } => local,
        other => panic!("lost local removal rotation: {other:?}"),
    };
    assert!(
        matches!(local, LocalRotation::Committed { mutation, .. } if mutation == row.intent_hash),
        "accepted removal must commit its retained rotation: {local:?}"
    );
    assert!(database.active_store_publication().await.unwrap().is_none());
}
